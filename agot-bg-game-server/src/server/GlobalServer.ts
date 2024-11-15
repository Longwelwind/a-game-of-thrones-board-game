import {Server} from "ws";
import {ClientMessage} from "../messages/ClientMessage";
import User from "./User";
import * as WebSocket from "ws";
import * as http from 'http';
import EntireGame, {SerializedEntireGame} from "../common/EntireGame";
import {ServerMessage} from "../messages/ServerMessage";
import WebsiteClient, { StoredGameData, StoredUserData } from "./website-client/WebsiteClient";
import LocalWebsiteClient from "./website-client/LocalWebsiteClient";
import LiveWebsiteClient from "./website-client/LiveWebsiteClient";
import BetterMap from "../utils/BetterMap";
import schema from "./ClientMessage.json";
import Ajv, {ValidateFunction} from "ajv";
import _ from "lodash";
import serializedGameMigrations from "./serializedGameMigrations";
import { v4 } from "uuid";
import sleep from "../utils/sleep";
import { compress, decompress } from "./utils/compression";
import * as Sentry from "@sentry/node"
import IngameGameState from "../common/ingame-game-state/IngameGameState";
import { getTimeDeltaInSeconds } from "../utils/getElapsedSeconds";

interface UserConnectionInfo {
    userId: string;
    userName: string;
    userIp: string;
    invalidatesAt?: Date;
}

export default class GlobalServer {
    server: Server;

    websiteClient: WebsiteClient;
    loadedGames = new BetterMap<string, EntireGame>();
    clientToUser: Map<WebSocket, User> = new Map<WebSocket, User>();
    clientMessageValidator: ValidateFunction;
    // The key of the outer map is the entireGameId. The key of the inner map is the socket id
    // So we can invalidate the correct UserConnectionInfo object after a defined period of time
    multiAccountingProtection = new BetterMap<string, BetterMap<string, UserConnectionInfo>>();
    socketIds = new BetterMap<WebSocket, string>();
    debug = false;

    get latestSerializedGameVersion(): string {
        const lastMigration = _.last(serializedGameMigrations);

        if (!lastMigration) {
            throw new Error();
        }

        return lastMigration.version;
    }

    constructor(server: Server) {
        this.server = server;

        if (process.env.MASTER_API_ENABLED != null) {
            console.log("Launching with live-website client");
            this.websiteClient = new LiveWebsiteClient();
        } else {
            console.log("Launching with local-website client");
            this.websiteClient = new LocalWebsiteClient();
            this.debug = true;
        }

        this.clientMessageValidator = new Ajv({allErrors: true}).compile(schema);
    }

    async start(): Promise<void> {
        this.server.on("connection", (client: WebSocket, incomingMsg: http.IncomingMessage) => {
            const clientIp = this.parseClientIp(incomingMsg);
            console.log("New user connected: " + clientIp);
            const socketId = v4();
            this.socketIds.set(client, socketId);
            client.on("message", (data: WebSocket.Data) => {
                this.onMessage(client, data as Buffer, clientIp, socketId);
            });
            client.on("close", () => {
                this.onClose(client);
            });
            client.on("error", console.error);
        });
    }

    onSendServerMessage(users: User[], message: ServerMessage): void {
        users.forEach(user => {
            user.connectedClients.forEach(connectedClient => {
                // It may happen that a connected is actually disconnected because,
                // disconnection happened after the beginning of the processing of
                // this function and thus the client was not removed properly off
                // the list.
                if (connectedClient.readyState == WebSocket.OPEN) {
                    this.send(connectedClient, message);
                }
            });
        });
    }

    async onMessage(client: WebSocket, data: Buffer, clientIp: string, socketId: string): Promise<void> {
        let message: ClientMessage | null = null;
        try {
            message = JSON.parse(decompress(data.toString())) as ClientMessage;
        } catch (error) {
            console.warn(`Error while parsing JSON for: ${data}`);
            console.log(error);
            return;
        }

        // Validate the JSON
        if (!this.clientMessageValidator(message)) {
            console.warn(`Unvalid schema of JSON message: ${data}, ${this.clientMessageValidator.errors}`);
            return;
        }

        if (message.type == "authenticate") {
            const {userId, requestUserId, gameId, authToken} = message.authData;

            // Check that the user exists
            const userData = await this.websiteClient.getUser(userId);

            if (!userData) {
                console.warn("Non-existing userId");
                return;
            }

            // Check that the token is good
            if (userData.token != authToken) {
                console.warn("Authentication with a wrong token");
                return;
            }

            // Get the game the user wants to authenticate to
            const entireGame = await this.getEntireGame(gameId);

            if (!entireGame) {
                console.warn("Authentication with a wrong gameId");
                return;
            }

            if (entireGame.ingameGameState?.bannedUsers.has(userData.id)) {
                return;
            }

            // Check if the game already contains this user
            const user = entireGame.users.has(userData.id)
                ? entireGame.users.get(userData.id)
                : entireGame.addUser(userData.id, userData.name, userData.profileSettings);

            // In case a user plays for another player, make sure to register the original request user
            // to the game as well to avoid chat messages from unknown users
            if (!entireGame.users.has(requestUserId)) {
                const requestUserData = await this.websiteClient.getUser(requestUserId);
                if (requestUserData) {
                    entireGame.addUser(requestUserData.id, requestUserData.name, requestUserData.profileSettings);
                }
            }

            const oldOtherUsersFromSameNetworkLength = user.otherUsersFromSameNetwork.length;
            user.otherUsersFromSameNetwork = _.union(user.otherUsersFromSameNetwork, this.getOtherUsersFromSameNetworkInSameGame(entireGame, userData, socketId, clientIp));

            if (oldOtherUsersFromSameNetworkLength < user.otherUsersFromSameNetwork.length) {
                entireGame.broadcastToClients({
                    type: "update-other-users-with-same-ip",
                    user: user.id,
                    otherUsers: user.otherUsersFromSameNetwork
                });
            }

            this.clientToUser.set(client, user);
            user.connectedClients.push(client);

            // The client might have disconnected between the execution of this thread,
            // and the sending of the response, thus why the state of the socket needs to
            // be checked here.
            if (client.readyState == WebSocket.OPEN) {
                this.send(client, {
                    type: "authenticate-response",
                    userId: user.id,
                    game: entireGame.serializeToClient(user)
                });
            }

            // Update the connection status for all other users
            user.updateConnectionStatus();
        } else if (message.type == "ping") {
            // The client may send ping to keep the connection alive.
            // Do nothing.
        } else {
            const user = this.clientToUser.get(client);

            if (!user) {
                console.error("Client sent a message other than \"authenticate\" but is not authenticated");
                return;
            }

            const entireGame = user.entireGame;

            console.log(`Message for game ${entireGame.id} from user ${user.id}:\n\t${JSON.stringify(message)}`);

            // Chat related messages are handled by GlobalServer because they must use the website client
            if (message.type == "create-private-chat-room") {
                const ingame = entireGame.childGameState instanceof IngameGameState ? entireGame.childGameState : null;
                if (!ingame || !ingame.players.has(user) || entireGame.gameSettings.noPrivateChats) {
                    return;
                }
                const otherUser = user.entireGame.users.get(message.otherUser);
                // Check if a chat room has not already been started between these 2 users
                const users = _.sortBy([user, otherUser], u => u.id);

                if (entireGame.privateChatRoomsIds.has(users[0]) && entireGame.privateChatRoomsIds.get(users[0]).has(users[1])) {
                    return;
                }

                // Create a chat room between these 2 players
                const roomId = await this.websiteClient.createPrivateChatRoom(users, `Chat room for ${users.map(u => u.name).join(" and ")} in game ${user.entireGame.id}`);

                if (!entireGame.privateChatRoomsIds.has(users[0])) {
                    entireGame.privateChatRoomsIds.set(users[0], new BetterMap());
                }

                entireGame.privateChatRoomsIds.get(users[0]).set(users[1], roomId);

                users.forEach(u => {
                    u.send({
                        type: "new-private-chat-room",
                        users: users.map(u => u.id),
                        roomId,
                        initiator: user.id
                    });
                });

                this.saveGame(entireGame, false);
            } else {
                entireGame.onClientMessage(user, message);
            }
        }

        const user = this.clientToUser.get(client);
        if (user) {
            user.entireGame.lastMessageReceivedAt = new Date();
        }
    }

    saveGame(entireGame: EntireGame, updateLastActive: boolean): void {
        const state = entireGame.getStateOfGame();
        const viewOfGame = entireGame.getViewOfGame();
        const players = entireGame.getPlayersInGame();
        const serializedGame = entireGame.serializeToClient(null);
        // Assume that all game always follow the latest serializedGame version,
        // since they have been migrated when loaded.
        const version = this.latestSerializedGameVersion;

        this.websiteClient.saveGame(entireGame.id, serializedGame, viewOfGame, players, state, version, updateLastActive);
    }

    restartLiveClockTimers(entireGame: EntireGame): void {
        if (entireGame.lobbyGameState) {
            // Set all players unready
            entireGame.lobbyGameState.readyUsers = null;
            entireGame.lobbyGameState.readyCheckWillTimeoutAt = null;
        }

        if (!entireGame.gameSettings.onlyLive || !entireGame.ingameGameState
            || entireGame.ingameGameState.isEndedOrCancelled) {
            return;
        }

        const now = new Date();
        const ingame = entireGame.ingameGameState;

        if (ingame.willBeAutoResumedAt) {
            // If server restarts during game pause, game must be resumed by vote
            ingame.willBeAutoResumedAt = null;
        }

        if (ingame.players.values.some(p => p.liveClockData == null)) {
            // Something really strange happened. Pause game, reinit all live clock datas and return;
            this.onCaptureSentryMessage("Player with liveClockData = null in onlyLive game detected", "error");
            ingame.players.values.forEach(p => p.liveClockData = {
                remainingSeconds: entireGame.gameSettings.initialLiveClock * 60,
                timerStartedAt: null,
                serverTimer: null
            });
            ingame.willBeAutoResumedAt = null;
            ingame.paused = now;
            return;
        }

        const clockDataPerPlayer = new BetterMap(ingame.players.values.map(p => {
            return [p, {
                    timerStartedAt: p.liveClockData?.timerStartedAt ?? null,
                    totalRemainingSeconds: p.totalRemainingSeconds
                }]
            })
        );

        if (clockDataPerPlayer.values.some(clockData =>
            clockData.timerStartedAt != null && clockData.totalRemainingSeconds == 0)
        ) {
            // A clock was started but server was offline for so long, that a clock ran out of time already
            // In this case we pause the game so it must be resumed by vote or cancelled.
            // Then we calculate the average remaining seconds of players with no active timer and assign it to the ones with an active timer
            ingame.paused = now;

            const playersWithNoActiveTimer = clockDataPerPlayer.entries
                .filter(([_p, clockData]) => clockData.timerStartedAt == null);

            if (playersWithNoActiveTimer.length == 0) {
                // Timers of all players were running => reinit all clocks
                clockDataPerPlayer.keys.forEach(p => {
                    clockDataPerPlayer.set(p, {
                        timerStartedAt: null, // We paused the game => Don't restart a timer
                        totalRemainingSeconds: entireGame.gameSettings.initialLiveClock * 60
                    });
                });
            } else {
                // Apply average to the players with active timer
                const avg = Math.round(_.sum(playersWithNoActiveTimer.map(([_p, clockData]) => clockData.totalRemainingSeconds)) / playersWithNoActiveTimer.length);
                clockDataPerPlayer.entries.forEach(([p, clockData]) => {
                    if (clockData.timerStartedAt != null) {
                        clockDataPerPlayer.set(p, {
                            timerStartedAt: null, // We paused the game => Don't restart a timer
                            totalRemainingSeconds: avg
                        });
                    }
                });
            }
        }

        clockDataPerPlayer.entries.forEach(([p, clockData]) => {
            p.liveClockData = {
                remainingSeconds: clockData.totalRemainingSeconds,
                timerStartedAt: clockData.timerStartedAt ? now : null,
                serverTimer: clockData.timerStartedAt
                    ? setTimeout(() => ingame.onPlayerClockTimeout(p), clockData.totalRemainingSeconds * 1000)
                    : null
            };
        });
    }

    async getEntireGame(gameId: string): Promise<EntireGame | null> {
        // Check if it has already been loaded
        if (this.loadedGames.has(gameId)) {
            return this.loadedGames.get(gameId);
        }

        // Otherwise, try to fetch it in the database
        const gameData = await this.websiteClient.getGame(gameId);
        if (!gameData) {
            return null;
        }

        const needsDeserialization = gameData.serializedGame != null;

        // Load it
        const entireGame = needsDeserialization
            ? this.deserializeStoredGame(gameData)
            : await this.createGame(gameData.id, gameData.ownerId, gameData.name);

        if (needsDeserialization) {
            try {
                this.restartLiveClockTimers(entireGame);
            } catch (e) {
                Sentry.captureException(e);
            }
        }

        // Bind listeners
        entireGame.onSendClientMessage = _ => {
            console.error("Server instance of ingame tried to send a client message");
        };

        entireGame.onSendServerMessage = (users, message) => this.onSendServerMessage(users, message);
        entireGame.onReadyToStart = (users) => this.onReadyToStart(entireGame, users);
        entireGame.onWaitedUsers = (users) => this.onWaitedUsers(entireGame, users);
        entireGame.onBattleResults = (users) => this.onBattleResults(entireGame, users);
        entireGame.onBribeForSupport = (users) => this.onBribeForSupport(entireGame, users);
        entireGame.onNewVoteStarted = (users) => this.onNewVoteStarted(entireGame, users);
        entireGame.onGameEnded = (users) => this.onGameEnded(entireGame, users);
        entireGame.onNewPbemResponseTime = (user, responseTimeInSeconds) => this.onNewPbemResponseTime(user, responseTimeInSeconds);
        entireGame.onClearChatRoom = (roomId) => this.onClearChatRoom(roomId);
        entireGame.onCaptureSentryMessage = (message, severity) => this.onCaptureSentryMessage(`${entireGame.id}: ${message}`, severity);
        entireGame.onSaveGame = (updateLastActive: boolean) => this.saveGame(entireGame, updateLastActive);
        entireGame.onGetUser = (userId) => this.websiteClient.getUser(userId);

        // Set the connection status of all users to false
        entireGame.users.values.forEach(u => u.connected = false);

        this.loadedGames.set(gameId, entireGame);

        return entireGame;
    }

    send(socket: WebSocket, message: ServerMessage): void {
        socket.send(compress(JSON.stringify(message)));
    }

    deserializeStoredGame(gameData: StoredGameData): EntireGame {
        // Check if the serialized game needs to be migrated
        if (gameData.version != this.latestSerializedGameVersion) {
            gameData.serializedGame = this.migrateSerializedGame(gameData.serializedGame, gameData.version as string);
        }

        return EntireGame.deserializeFromServer(gameData.serializedGame as SerializedEntireGame);
    }

    // eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
    migrateSerializedGame(serializedGame: any, version: string): any {
        const migrationI = serializedGameMigrations.findIndex(m => m.version == version);

        const migrationsToApply = serializedGameMigrations.slice(migrationI + 1);

        const migratedSerializedGame = migrationsToApply.reduce(
            (serializedGame, migration) => migration.migrate(serializedGame),
            serializedGame
        );

        return migratedSerializedGame;
    }

    async createGame(id: string, ownedId: string, name: string): Promise<EntireGame> {
        // Create a public chat room ID
        const publicChatRoomId = await this.websiteClient.createPublicChatRoom(`Chat for game ${id}`);

        const entireGame = new EntireGame(id, ownedId, name);
        entireGame.publicChatRoomId = publicChatRoomId;
        entireGame.firstStart();

        return entireGame;
    }

    parseClientIp(incomingMessage: http.IncomingMessage): string {
        const xForwardedForHeader = incomingMessage.headers['x-forwarded-for'];
        if (!xForwardedForHeader) {
            return incomingMessage.socket.remoteAddress ? incomingMessage.socket.remoteAddress : "";
        }

        let xForwardedFor = "";
        if (Array.isArray(xForwardedForHeader) && xForwardedForHeader.length > 0) {
            xForwardedFor = xForwardedForHeader.shift() as string;
        }

        if (typeof xForwardedForHeader === "string") {
            xForwardedFor = xForwardedForHeader;
        }

        const firstValue = xForwardedFor.split(',').shift();
        return firstValue ? firstValue.trim() : "";
    }

    onReadyToStart(game: EntireGame, users: User[]): void {
        this.websiteClient.notifyReadyToStart(game.id, users.map(u => u.id));
    }

    onWaitedUsers(game: EntireGame, users: User[]): void {
        const offlineUsers = users.filter(u => !u.connected || this.debug);
        if (offlineUsers.length == 0) {
            return;
        }

        this.websiteClient.notifyYourTurn(game.id, offlineUsers.map(u => u.id));
    }

    onBribeForSupport(game: EntireGame, users: User[]): void {
        const offlineUsers = users.filter(u => !u.connected || this.debug);
        if (offlineUsers.length == 0) {
            return;
        }

        this.websiteClient.notifyBribeForSupport(game.id, offlineUsers.map(u => u.id));
    }

    onBattleResults(game: EntireGame, users: User[]): void {
        const offlineUsers = users.filter(u => !u.connected || this.debug);
        if (offlineUsers.length == 0) {
            return;
        }

        this.websiteClient.notifyBattleResults(game.id, offlineUsers.map(u => u.id));
    }

    onNewVoteStarted(game: EntireGame, users: User[]): void {
        const offlineUsers = users.filter(u => !u.connected || this.debug);
        if (offlineUsers.length == 0) {
            return;
        }

        this.websiteClient.notifyNewVote(game.id, offlineUsers.map(u => u.id));
    }

    onGameEnded(game: EntireGame, users: User[]): any {
        this.websiteClient.notifyGameEnded(game.id, users.map(u => u.id));
    }

    onNewPbemResponseTime(user: User, responseTimeInSeconds: number): void {
        this.websiteClient.addPbemResponseTime(user, responseTimeInSeconds);
    }

    onClearChatRoom(roomId: string): void {
        this.websiteClient.clearChatRoom(roomId);
    }

    onCaptureSentryMessage(message: string, severity: "info" | "warning" | "error" | "fatal"): void {
        Sentry.captureMessage(message, severity as Sentry.Severity);
    }

    onClose(client: WebSocket): void {
        if (this.clientToUser.has(client)) {
            const user = this.clientToUser.get(client) as User;
            user.connectedClients.splice(user.connectedClients.indexOf(client), 1);
            this.clientToUser.delete(client);

            user.updateConnectionStatus();

            //this.addIpToMultiAccountProtectionMap(client, user);
        }
    }

    private addIpToMultiAccountProtectionMap(client: WebSocket, user: User): void {
        if (this.socketIds.has(client)) {
            const socketId = this.socketIds.get(client);
            if (this.multiAccountingProtection.has(user.entireGame.id)) {
                const userConnectionInfos = this.multiAccountingProtection.get(user.entireGame.id);
                if (userConnectionInfos.has(socketId)) {
                    const date = new Date();
                    date.setHours(date.getHours() + 16);
                    userConnectionInfos.get(socketId).invalidatesAt = date;
                }
            }
            this.socketIds.delete(client);
        }
    }

    getOtherUsersFromSameNetworkInSameGame(entireGame: EntireGame, userData: StoredUserData, socketId: string, clientIp: string): string[] {
        return [];

        // TODO
        if (!this.multiAccountingProtection.has(entireGame.id)) {
            this.multiAccountingProtection.set(entireGame.id, new BetterMap());
        }

        const userConnectionInfos = this.multiAccountingProtection.get(entireGame.id);
        userConnectionInfos.set(socketId, {
            userId: userData.id,
            userName: userData.name,
            userIp: clientIp
        });

        const otherUsersWithSameIp = userConnectionInfos.values.filter(
            uci => uci.userId != userData.id && uci.userIp == clientIp
        );

        return otherUsersWithSameIp.map(uci => uci.userName);
    }

    unloadGame(entireGame: EntireGame) {
        if (!this.loadedGames.has(entireGame.id)) {
            console.warn("Tried to unload game that was not loaded: " + entireGame.id);
            return;
        }

        if (entireGame.gameSettings.onlyLive && entireGame.ingameGameState) {
            const ingame = entireGame.ingameGameState;
            if (!ingame.isEndedOrCancelled) {
                // Do not unload running clock games until they are finished
                return;
            }
        }

        console.log("Unloading game " + entireGame.id);

        // Save the game before unloading:
        if (entireGame.onSaveGame) {
            entireGame.onSaveGame(false);
        }

        // As we keep the game active as long as we receive "ping" this is only kept for safety
        entireGame.users.values.forEach(u => {
            u.connectedClients.forEach(ws => {
                ws.close();
                setTimeout(() => {
                    if (ws.readyState == WebSocket.OPEN || ws.readyState == WebSocket.CLOSING) {
                        ws.terminate();
                    }
                  }, 10000);
            });
        });

        entireGame.onSendClientMessage = undefined;
        entireGame.onSendServerMessage = undefined;
        entireGame.onReadyToStart = undefined;
        entireGame.onWaitedUsers = undefined;
        entireGame.onBattleResults = undefined;
        entireGame.onBribeForSupport = undefined;
        entireGame.onNewVoteStarted = undefined;
        entireGame.onGameEnded = undefined;
        entireGame.onNewPbemResponseTime = undefined
        entireGame.onClearChatRoom = undefined;
        entireGame.onCaptureSentryMessage = undefined;
        entireGame.onSaveGame = undefined;
        entireGame.onGetUser = undefined;

        this.loadedGames.delete(entireGame.id);
    }

    async runBackgroundTasks(): Promise<void> {
        while (true) {
            await sleep(3 * 60 * 1000);

            const now = new Date();

            // Unload inactive games:
            this.loadedGames.values.filter(game => game.lastMessageReceivedAt != null).forEach(game => {
                const secondsSinceLastIncomingMessage = getTimeDeltaInSeconds(now, game.lastMessageReceivedAt as Date);
                if ((game.gameSettings.pbem && secondsSinceLastIncomingMessage >= 60) ||
                    (!game.gameSettings.pbem && secondsSinceLastIncomingMessage >= (35 * 60))) {
                    this.unloadGame(game);
                }
            });

            // Invalidate outdated Ip entries:
            /*
            this.multiAccountingProtection.keys.forEach(entireGameId => {
                const userConnectionInfos = this.multiAccountingProtection.get(entireGameId);
                userConnectionInfos.keys.forEach(socketId => {
                    const uci = userConnectionInfos.get(socketId);
                    if (uci.invalidatesAt && now.getTime() > uci.invalidatesAt.getTime()) {
                        userConnectionInfos.delete(socketId);
                    }
                });

                if (userConnectionInfos.size == 0) {
                    this.multiAccountingProtection.delete(entireGameId);
                }
            });
            */
        }
    }
}