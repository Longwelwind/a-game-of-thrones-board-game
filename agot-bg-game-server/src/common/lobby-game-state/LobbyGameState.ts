import EntireGame from "../EntireGame";
import GameState from "../GameState";
import User from "../../server/User";
import {ClientMessage} from "../../messages/ClientMessage";
import {ServerMessage} from "../../messages/ServerMessage";
import {observable} from "mobx";
import BetterMap from "../../utils/BetterMap";
import baseGameData from "../../../data/baseGameData.json";
import CancelledGameState from "../cancelled-game-state/CancelledGameState";
import shuffle from "../../utils/shuffle";
import _ from "lodash";
import { MIN_PLAYER_COUNT_WITH_VASSALS } from "../ingame-game-state/game-data-structure/Game";

export default class LobbyGameState extends GameState<EntireGame> {
    lobbyHouses: BetterMap<string, LobbyHouse>;
    @observable players = new BetterMap<LobbyHouse, User>();

    get entireGame(): EntireGame {
        return this.parentGameState;
    }

    firstStart(): void {
        // Load the available houses for this game
        this.lobbyHouses = this.getLobbyHouses();
    }

    getLobbyHouses(): BetterMap<string, LobbyHouse> {
        return new BetterMap(
            Object.entries(baseGameData.houses)
                .map(([hid, h]) => [hid, {id: hid, name: h.name, color: h.color}])
        );
    }

    getAvailableHouses(): LobbyHouse[] {
        return this.lobbyHouses.values.filter(h => this.entireGame.selectedGameSetup.houses.includes(h.id));
    }

    onGameSettingsChange(): void {
        // Remove all chosen houses that are not available with the new settings
        const availableHouses = this.getAvailableHouses();
        const usersForReassignment: User[] = [];

        let dirty = false;
        this.players.keys.forEach(house => {
            if (!availableHouses.includes(house)) {
                dirty = true;
                usersForReassignment.push(this.players.get(house));
                this.players.delete(house);
            }
        });

        if (usersForReassignment.length > 0 && this.players.size < this.entireGame.selectedGameSetup.playerCount) {
            const freeHouses = _.difference(availableHouses, this.players.keys);

            while (freeHouses.length > 0 && usersForReassignment.length > 0) {
                this.players.set(freeHouses.shift() as LobbyHouse, usersForReassignment.shift() as User);
            }
        }

        if (dirty) {
            this.entireGame.broadcastToClients({
                type: "house-chosen",
                players: this.players.entries.map(([house, user]) => [house.id, user.id])
            });
        }
    }

    onClientMessage(user: User, message: ClientMessage): void {
        if (message.type == "launch-game") {
            if (!this.entireGame.isOwner(user)) {
                return;
            }

            if (!this.canStartGame(user).success) {
                return;
            }

            if (this.entireGame.gameSettings.randomHouses) {
                if (this.entireGame.gameSettings.vassals) {
                    // Assign a random house to the players
                    const allShuffledHouses = _.shuffle(this.getAvailableHouses());
                    const connectedUsers = this.players.values;
                    this.players = new BetterMap();
                    for(const user of connectedUsers) {
                        this.players.set(allShuffledHouses.splice(0, 1)[0], user);
                    }
                } else {
                    const shuffled = shuffle(this.players.entries);

                    const lobbyHouses = this.players.keys;
                    for (let i = 0; i < shuffled.length; i++) {
                        this.players.set(lobbyHouses[i], shuffled[i][1]);
                    }
                }
            }

            this.entireGame.proceedToIngameGameState(
                this.getAvailableHouses().map(h => h.id),
                new BetterMap(this.players.map((h, u) => ([h.id, u])))
            );
        } else if (message.type == "kick-player") {
            const kickedUser = this.entireGame.users.get(message.user);

            if (!this.entireGame.isOwner(user) || kickedUser == user) {
                return;
            }

            this.setUserForLobbyHouse(null, kickedUser);
        } else if (message.type == "cancel-game") {
            if (!this.entireGame.isOwner(user)) {
                return;
            }

            this.entireGame.setChildGameState(new CancelledGameState(this.entireGame)).firstStart();
        } else if (message.type == "choose-house") {
            const house = message.house ? this.lobbyHouses.get(message.house) : null;

            // Check if the house is available
            if (house && (this.players.has(house) || !this.getAvailableHouses().includes(house))) {
                return;
            }

            this.setUserForLobbyHouse(house, user);
        }
    }

    setUserForLobbyHouse(house: LobbyHouse | null, user: User): void {
        this.players.forEach((houseUser, house) => {
            if (user == houseUser) {
                this.players.delete(house);
            }
        });

        if (house) {
            this.players.set(house, user);
        }

        this.entireGame.broadcastToClients({
            type: "house-chosen",
            players: this.players.entries.map(([house, user]) => [house.id, user.id])
        });
    }

    canStartGame(user: User): {success: boolean; reason: string} {
        if (!this.entireGame.isOwner(user)) {
            return {success: false, reason: "not-owner"};
        }

        // If Vassals are toggled we need at least min_player_count_with_vassals
        if (this.entireGame.gameSettings.vassals) {
            if (this.players.size < MIN_PLAYER_COUNT_WITH_VASSALS) {
                return {success: false, reason: "not-enough-players"};
            }
        } else if (this.players.size < this.entireGame.selectedGameSetup.playerCount) {
            return {success: false, reason: "not-enough-players"};
        }

        return {success: true, reason: "ok"};
    }

    canCancel(user: User):  {success: boolean; reason: string} {
        if (!this.entireGame.isRealOwner(user)) {
            return {success: false, reason: "not-owner"};
        }

        return {success: true, reason: "ok"};
    }

    onServerMessage(message: ServerMessage): void {
        if (message.type == "house-chosen") {
            this.players = new BetterMap(message.players.map(([hid, uid]) => [
                this.lobbyHouses.get(hid),
                this.entireGame.users.get(uid)
            ]));

            if (this.entireGame.onClientGameStateChange) {
                // Fake a game state change to play a sound also in case lobby is full
                this.entireGame.onClientGameStateChange();
            }
        }
    }

    chooseHouse(house: LobbyHouse | null): void {
        this.entireGame.sendMessageToServer({
            type: "choose-house",
            house: house ? house.id : null
        });
    }

    start(): void {
        this.entireGame.sendMessageToServer({
            type: "launch-game"
        });
    }

    cancel(): void {
        this.entireGame.sendMessageToServer({
            type: "cancel-game"
        });
    }

    kick(user: User): void {
        this.entireGame.sendMessageToServer({
            type: "kick-player",
            user: user.id
        });
    }

    getWaitedUsers(): User[] {
        const owner = this.entireGame.owner;
        if (!owner || !this.canStartGame(owner).success) {
            return [];
        }

        return [owner];
    }

    serializeToClient(_admin: boolean, _user: User | null): SerializedLobbyGameState {
        return {
            type: "lobby",
            lobbyHouses: this.lobbyHouses.values,
            players: this.players.entries.map(([h, u]) => [h.id, u.id])
        };
    }

    static deserializeFromServer(entireGame: EntireGame, data: SerializedLobbyGameState): LobbyGameState {
        const lobbyGameState = new LobbyGameState(entireGame);

        lobbyGameState.lobbyHouses = new BetterMap(data.lobbyHouses.map(h => [h.id, h]));
        lobbyGameState.players = new BetterMap(data["players"].map(([hid, uid]) => [lobbyGameState.lobbyHouses.get(hid), entireGame.users.get(uid)]));

        return lobbyGameState;
    }
}

export interface SerializedLobbyGameState {
    type: "lobby";
    players: [string, string][];
    lobbyHouses: LobbyHouse[];
}

export interface LobbyHouse {
    id: string;
    name: string;
    color: string;
}
