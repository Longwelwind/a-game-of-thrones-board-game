import EntireGame, { NotificationType } from "../EntireGame";
import GameState from "../GameState";
import {ClientMessage} from "../../messages/ClientMessage";
import {ServerMessage} from "../../messages/ServerMessage";
import User from "../../server/User";
import World from "./game-data-structure/World";
import Player, {SerializedPlayer} from "./Player";
import Region, { RegionState } from "./game-data-structure/Region";
import PlanningGameState, {SerializedPlanningGameState} from "./planning-game-state/PlanningGameState";
import ActionGameState, {SerializedActionGameState} from "./action-game-state/ActionGameState";
import Order from "./game-data-structure/Order";
import Game, {SerializedGame} from "./game-data-structure/Game";
import WesterosGameState, {SerializedWesterosGameState} from "./westeros-game-state/WesterosGameState";
import createGame, { applyChangesForDanceWithMotherOfDragons } from "./game-data-structure/createGame";
import BetterMap from "../../utils/BetterMap";
import House from "./game-data-structure/House";
import Unit from "./game-data-structure/Unit";
import PlanningRestriction from "./game-data-structure/westeros-card/planning-restriction/PlanningRestriction";
import GameLogManager, {SerializedGameLogManager} from "./game-data-structure/GameLogManager";
import {GameLogData} from "./game-data-structure/GameLog";
import GameEndedGameState, {SerializedGameEndedGameState} from "./game-ended-game-state/GameEndedGameState";
import UnitType from "./game-data-structure/UnitType";
import WesterosCard from "./game-data-structure/westeros-card/WesterosCard";
import Vote, { SerializedVote, VoteState } from "./vote-system/Vote";
import VoteType, { CancelGame, EndGame, ExtendPlayerClocks, PauseGame, ReplacePlayer, ReplacePlayerByVassal, ReplaceVassalByPlayer, ResumeGame, SwapHouses } from "./vote-system/VoteType";
import { v4 } from "uuid";
import CancelledGameState, { SerializedCancelledGameState } from "../cancelled-game-state/CancelledGameState";
import HouseCard from "./game-data-structure/house-card/HouseCard";
import { observable } from "mobx";
import _ from "lodash";
import DraftHouseCardsGameState, { houseCardCombatStrengthAllocations, SerializedDraftHouseCardsGameState } from "./draft-house-cards-game-state/DraftHouseCardsGameState";
import CombatGameState from "./action-game-state/resolve-march-order-game-state/combat-game-state/CombatGameState";
import DeclareSupportGameState from "./action-game-state/resolve-march-order-game-state/combat-game-state/declare-support-game-state/DeclareSupportGameState";
import ThematicDraftHouseCardsGameState, { SerializedThematicDraftHouseCardsGameState } from "./thematic-draft-house-cards-game-state/ThematicDraftHouseCardsGameState";
import shuffle from "../../utils/shuffle";
import shuffleInPlace from "../../utils/shuffleInPlace";
import popRandom from "../../utils/popRandom";
import LoanCard from "./game-data-structure/loan-card/LoanCard";
import PayDebtsGameState, { SerializedPayDebtsGameState } from "./pay-debts-game-state/PayDebtsGameState";
import { objectiveCards } from "./game-data-structure/static-data-structure/objectiveCards";
import ChooseInitialObjectivesGameState, { SerializedChooseInitialObjectivesGameState } from "./choose-initial-objectives-game-state/ChooseInitialObjectivesGameState";
import facelessMenNames from "../../../data/facelessMenNames.json";
import WildlingCardEffectInTurnOrderGameState from "./westeros-game-state/wildlings-attack-game-state/WildlingCardEffectInTurnOrderGameState";
import getElapsedSeconds from "../../utils/getElapsedSeconds";
import orders from "./game-data-structure/orders";
import { OrderOnMapProperties, UnitOnMapProperties } from "../../client/MapControls";

export const NOTE_MAX_LENGTH = 5000;

export const enum ReplacementReason {
    VOTE,
    CLOCK_TIMEOUT
}

export default class IngameGameState extends GameState<
    EntireGame,
    WesterosGameState | PlanningGameState | ActionGameState | CancelledGameState | GameEndedGameState
    | DraftHouseCardsGameState | ThematicDraftHouseCardsGameState | PayDebtsGameState
    | ChooseInitialObjectivesGameState
> {
    players: BetterMap<User, Player> = new BetterMap();
    oldPlayerIds: string[] = [];
    replacerIds: string[] = [];
    timeoutPlayerIds: string[] = [];
    game: Game;
    gameLogManager: GameLogManager = new GameLogManager(this);
    @observable ordersOnBoard: BetterMap<Region, Order> = new BetterMap();

    votes: BetterMap<string, Vote> = new BetterMap();
    @observable paused: Date | null = null;
    @observable willBeAutoResumedAt: Date | null = null;

    // Server-side only
    autoResumeTimeout: NodeJS.Timeout | null = null;

    // Client-side only
    @observable rerender = 0;
    @observable now = new Date();
    @observable marchMarkers: BetterMap<Unit, Region> = new BetterMap();
    @observable unitsToBeAnimated: BetterMap<Unit, UnitOnMapProperties> = new BetterMap();
    @observable ordersToBeAnimated: BetterMap<Region, OrderOnMapProperties> = new BetterMap();
    @observable replayWorldState: RegionState[] | null = null;

    onVoteStarted: (() => void) | null = null;
    onPreemptiveRaidNewAttack: ((biddings: [number, House[]][], highestBidder: House) => void) | null = null;
    onLogReceived: ((log: GameLogData) => void) | null = null;

    get entireGame(): EntireGame {
        return this.parentGameState;
    }

    get world(): World {
        return this.game.world;
    }

    get actionState(): ActionGameState | null {
        return this.childGameState as ActionGameState;
    }

    get sortedByLeadingPlayers(): Player[] {
        return this.game.getPotentialWinners().map(h => this.getControllerOfHouse(h));
    }

    get isEnded(): boolean {
        return this.childGameState instanceof GameEndedGameState;
    }

    get isCancelled(): boolean {
        return this.childGameState instanceof CancelledGameState;
    }

    constructor(entireGame: EntireGame) {
        super(entireGame);
    }

    beginGame(housesToCreate: string[], futurePlayers: BetterMap<string, User>): void {
        this.entireGame.broadcastToClients({
            type: "game-started"
        });

        this.game = createGame(this, housesToCreate, futurePlayers.keys);
        this.players = new BetterMap(futurePlayers.map((house, user) => [user, new Player(user, this.game.houses.get(house))]));

        if (this.entireGame.isDanceWithMotherOfDragons) {
            applyChangesForDanceWithMotherOfDragons(this);
        }

        if (this.entireGame.gameSettings.onlyLive) {
            this.players.values.forEach(p => p.liveClockData = {
                remainingSeconds: this.entireGame.gameSettings.initialLiveClock * 60,
                timerStartedAt: null,
                serverTimer: null
            });
        }

        // In the past we always used the supply limits from the game setup, though we simply could have calculated them
        // as every house starts according to their controlled barrels.
        // For the custom settings "Random Start Positions" and "Vassal Start Positions"
        // we now have to calculate the supply limits of player houses in the beginning. (Vassals always start at supply level 4)
        this.game.nonVassalHouses.forEach(h =>  {
            h.supplyLevel = Math.min(this.game.supplyRestrictions.length - 1, this.game.getControlledSupplyIcons(h));
        });

        this.log({
            type: "user-house-assignments",
            assignments: futurePlayers.map((house, user) => [house, user.id]) as [string, string][]
        });

        if (this.entireGame.gameSettings.draftHouseCards) {
            this.beginDraftingHouseCards();
        } else if (this.entireGame.isFeastForCrows) {
            this.chooseObjectives();
        } else {
            this.beginNewRound();
        }
    }

    assignNewFacelessNames(): void {
        if (this.entireGame.gameSettings.faceless) {
            const facelessNames: string[] = [...facelessMenNames];
            this.players.values.forEach(p => p.user.facelessName = popRandom(facelessNames) ?? p.user.facelessName);
            this.entireGame.hideOrRevealUserNames(false);
        }
    }

    chooseObjectives(): void {
        this.setChildGameState(new ChooseInitialObjectivesGameState(this)).firstStart();
    }

    onChooseInitialObjectivesGameStateEnd(): void {
        this.beginNewRound();
    }

    beginDraftingHouseCards(): void {
        if (this.entireGame.gameSettings.thematicDraft) {
            this.setChildGameState(new ThematicDraftHouseCardsGameState(this)).firstStart();
        } else if (this.entireGame.gameSettings.blindDraft) {
            houseCardCombatStrengthAllocations.entries.forEach(([hcStrength, count]) => {
                for(let i=0; i<count; i++) {
                    this.players.values.forEach(p => {
                        const house = p.house;
                        const availableCards = this.game.houseCardsForDrafting.values.filter(hc => hc.combatStrength == hcStrength);
                        const houseCard = popRandom(availableCards) as HouseCard;
                        house.houseCards.set(houseCard.id, houseCard);
                        this.game.houseCardsForDrafting.delete(houseCard.id);
                    });
                }
            });

            this.game.houseCardsForDrafting.clear();

            this.setInfluenceTrack(0, this.getRandomTrackOrder());
            this.setInfluenceTrack(1, this.getRandomTrackOrder());
            this.setInfluenceTrack(2, this.getRandomTrackOrder());

            this.onDraftingFinish();
        } else {
            this.setChildGameState(new DraftHouseCardsGameState(this)).firstStart();
        }
    }

    setInfluenceTrack(i: number, track: House[]): House[] {
        const fixedTrack = this.getFixedInfluenceTrack(track);
        if (i == 0) {
            this.game.ironThroneTrack = fixedTrack;
        } else if (i == 1) {
            this.game.fiefdomsTrack = fixedTrack;
        } else if (i == 2) {
            this.game.kingsCourtTrack = fixedTrack;
        } else {
            throw new Error();
        }

        this.entireGame.broadcastToClients({
            type: "change-tracker",
            trackerI: i,
            tracker: fixedTrack.map(h => h.id)
        });

        return fixedTrack;
    }

    getFixedInfluenceTrack(track: House[]): House[] {
        if (!this.game.targaryen) {
            return track;
        }

        return _.concat(_.without(track, this.game.targaryen), this.game.targaryen);
    }

    getRandomTrackOrder(): House[] {
        const playerHouses = this.game.houses.values.filter(h => !this.isVassalHouse(h));
        const vassalHouses = _.without(this.game.houses.values, ...playerHouses);

        return _.concat(shuffleInPlace(playerHouses), shuffleInPlace(vassalHouses));
    }

    log(data: GameLogData, resolvedAutomatically = false): void {
        this.gameLogManager.log(data, resolvedAutomatically);
    }

    onDraftingFinish(): void {
        if (this.entireGame.isFeastForCrows) {
            this.chooseObjectives();
        } else {
            this.beginNewRound();
        }
    }

    onActionGameStateFinish(): void {
        this.beginNewRound();
    }

    onWesterosGameStateFinish(planningRestrictions: PlanningRestriction[], revealedWesterosCards: WesterosCard[]): void {
        this.proceedPlanningGameState(planningRestrictions, revealedWesterosCards);
    }

    proceedPlanningGameState(planningRestrictions: PlanningRestriction[] = [], revealedWesterosCards: WesterosCard[] = []): void {
        this.game.vassalRelations = new BetterMap();
        this.broadcastVassalRelations();
        this.setChildGameState(new PlanningGameState(this)).firstStart(planningRestrictions, revealedWesterosCards);
    }

    proceedToActionGameState(placedOrders: BetterMap<Region, Order>, planningRestrictions: PlanningRestriction[]): void {
        this.ordersOnBoard = placedOrders;

        this.entireGame.broadcastToClients({
            type: "reveal-orders",
            orders: placedOrders.mapOver(r => r.id, o => o.id)
        });

        this.setChildGameState(new ActionGameState(this)).firstStart(planningRestrictions);
    }

    beginNewRound(): void {
        if (this.game.turn == this.game.maxTurns) {
            const winner = this.game.getPotentialWinner(true);
            this.setChildGameState(new GameEndedGameState(this)).firstStart(winner);
            return;
        }

        if (this.ordersOnBoard.size > 0) {
            this.entireGame.broadcastToClients({
                type: "remove-orders",
                regions: this.ordersOnBoard.keys.map(r => r.id)
            });

            this.ordersOnBoard.clear();
        }

        if (this.game.ironBank) {
            this.game.ironBank.drawNewLoanCard();
        }

        if (this.game.turn != 0 && this.game.turn % 10 == 0) {
            // Refresh Westeros deck 3 after every 10th round
            const deck3 = this.game.westerosDecks[2];
            deck3.forEach(wc => wc.discarded = false);
            this.game.westerosDecks[2] = shuffle(deck3);

            this.broadcastWesterosDecks();

            // Reshuffle the wildling deck
            this.game.wildlingDeck = shuffle(this.game.wildlingDeck);
            this.game.houses.forEach(h => h.knowsNextWildlingCard = false);
            this.entireGame.broadcastToClients({type: "hide-top-wildling-card"});

            // Reshuffle the loan deck
            if (this.game.ironBank) {
                shuffleInPlace(this.game.ironBank.loanCardDeck);
                this.game.ironBank.loanCardDeck.forEach(lc => lc.discarded = false);
                this.game.ironBank.sendUpdateLoanCards();
            }
        }

        this.game.turn++;
        this.log({type: "turn-begin", turn: this.game.turn});

        this.game.valyrianSteelBladeUsed = false;

        // Unwound each units
        this.world.regions.forEach(r => r.units.forEach(u => u.wounded = false));

        for (let i = 0; i < this.game.winterIsComingHappened.length; i++) {
            this.game.winterIsComingHappened[i] = false;
        }

        this.entireGame.broadcastToClients({
            type: "new-turn"
        });


        if (this.game.turn > 1) {
            const unpaidInterest = this.game.ironBank?.payInterest() ?? []
            if (unpaidInterest.length == 0) {
                this.setChildGameState(new WesterosGameState(this)).firstStart();
            } else {
                this.setChildGameState(new PayDebtsGameState(this)).firstStart(unpaidInterest);
            }
        } else if (this.entireGame.isDanceWithMotherOfDragons) {
            // Reveal top 3 Westeros deck 4 cards
            this.setChildGameState(new WesterosGameState(this)).firstStart(true);
        } else {
            // No Westeros phase during the first turn
            this.proceedPlanningGameState();
        }
    }

    onPayDebtsGameStateFinish(): void {
        // The decider may remove a unit in an enemy home town.
        // If the enemy regains this castle, he might win the game.
        if (this.checkVictoryConditions()) {
            return;
        }

        this.setChildGameState(new WesterosGameState(this)).firstStart();
    }

    gainLoyaltyTokens(): void {
        const targaryen = this.game.targaryen;
        if (targaryen) {
            this.world.regions.values.filter(r => r.loyaltyTokens > 0 && r.getController() == targaryen).forEach(r => {
                targaryen.gainedLoyaltyTokens += r.loyaltyTokens;

                this.entireGame.broadcastToClients({
                    type: "loyalty-token-gained",
                    house: targaryen.id,
                    newLoyaltyTokenCount: targaryen.gainedLoyaltyTokens,
                    region: r.id
                });

                this.log({
                    type: "loyalty-token-gained",
                    house: targaryen.id,
                    count: r.loyaltyTokens,
                    region: r.id
                });

                r.loyaltyTokens = 0;
            });
        }
    }

    getFreeFacelessName(): string | null {
        const freeFacelessNames: string[] = _.difference(facelessMenNames, this.players.values.map(p => p.user.facelessName));
        return popRandom(freeFacelessNames);
    }

    cancelPendingReplaceVotes(): void {
        this.votes.values.forEach(v => {
            if (v.state == VoteState.ONGOING && v.isReplaceVoteType) {
                v.cancelVote();
            }
        });
    }

    onClientMessage(user: User, message: ClientMessage): boolean {
        if (message.type == "launch-replace-player-vote") {
            const player = this.players.get(this.entireGame.users.get(message.player));

            if (!this.canLaunchReplacePlayerVote(user).result) {
                return false;
            }

            this.createVote(user, new ReplacePlayer(user, player.user, player.house));
        } else if (message.type == "launch-replace-vassal-by-player-vote") {
            const house = this.game.houses.get(message.house);

            if (!this.canLaunchReplaceVassalVote(user, house).result) {
                return false;
            }

            this.createVote(user, new ReplaceVassalByPlayer(user, house));
        } else if (message.type == "game-log-seen") {
            this.gameLogManager.lastSeenLogTimes.set(user, message.time);
        } else if (this.players.has(user)) {
            const player = this.players.get(user);

            this.onPlayerMessage(player, message);
            return !message.type.includes("vote");
        }

        return false;
    }

    onPlayerMessage(player: Player, message: ClientMessage): void {
        if (message.type == "vote") {
            const vote = this.votes.get(message.vote);

            if (vote.state != VoteState.ONGOING ||
                !vote.participatingHouses.includes(player.house) ||
                !vote.canVote.result) {
                return;
            }

            vote.votes.set(player.house, message.choice);

            this.entireGame.broadcastToClients({
                type: "vote-done",
                vote: vote.id,
                voter: player.house.id,
                choice: message.choice
            });

            vote.checkVoteFinished();
        } else if (message.type == "launch-resume-game-vote") {
            if (this.canLaunchResumeGameVote(player).result) {
                this.createVote(
                    player.user,
                    new ResumeGame()
                );
            }
        }
        else if (message.type == "update-note") {
            player.user.note = message.note.substring(0, NOTE_MAX_LENGTH);
        } else if (message.type == "launch-swap-houses-vote") {
            const swappingPlayer = this.players.get(this.entireGame.users.get(message.swappingUser));
            if (this.canLaunchSwapHousesVote(player.user, swappingPlayer).result) {
                this.createVote(
                    player.user,
                    new SwapHouses(player.user, swappingPlayer.user, player.house, swappingPlayer.house)
                );
            }
        }

        if (this.paused) {
            return;
        }

        if (message.type == "launch-pause-game-vote") {
            if (this.canLaunchPauseGameVote(player).result) {
                this.createVote(
                    player.user,
                    new PauseGame()
                );
            }
        } else  if (message.type == "launch-cancel-game-vote") {
            if (this.canLaunchCancelGameVote(player).result) {
                this.createVote(
                    player.user,
                    new CancelGame()
                );
            }
        } else if (message.type == "launch-end-game-vote") {
            if (this.canLaunchEndGameVote(player).result) {
                this.createVote(
                    player.user,
                    new EndGame()
                );
            }
        } else if (message.type == "launch-extend-player-clocks-vote") {
            if (this.canLaunchExtendPlayerClocksVote(player).result) {
                this.createVote(
                    player.user,
                    new ExtendPlayerClocks()
                );
            }
        } else if (message.type == "launch-replace-player-by-vassal-vote") {
            const playerToReplace = this.players.get(this.entireGame.users.get(message.player));

            if (!this.canLaunchReplacePlayerVote(player.user, true).result) {
                return;
            }

            this.createVote(player.user, new ReplacePlayerByVassal(playerToReplace.user, playerToReplace.house));
        } else if (message.type == "gift-power-tokens") {
            if (!this.canGiftPowerTokens(player.house)) {
                return;
            }

            const toHouse = this.game.houses.get(message.toHouse);

            if (!this.isVassalHouse(toHouse)
                    && player.house != toHouse
                    && message.powerTokens > 0
                    && message.powerTokens <= player.house.powerTokens) {
                const delta = Math.abs(this.changePowerTokens(toHouse, message.powerTokens));
                this.changePowerTokens(player.house, -delta);
                this.log({
                    type: "power-tokens-gifted",
                    house: player.house.id,
                    affectedHouse: toHouse.id,
                    powerTokens: message.powerTokens
                });
            }
        } else if (message.type == "drop-power-tokens") {
            // Only allow Targ to drop their Power tokens
            if (player.house != this.game.targaryen || !this.isHouseDefeated(player.house)) {
                return;
            }

            this.changePowerTokens(player.house, -player.house.powerTokens);
        } else {
            this.childGameState.onPlayerMessage(player, message);
        }
    }

    setWaitedForPlayers(previouslyWaitedFor: Player[]): void {
        if (!this.entireGame.gameSettings.pbem) {
            return;
        }

        this.players.values.forEach(p => {
            const isWaitedFor = this.leafState.getWaitedUsers().includes(p.user);

            if (isWaitedFor && !p.waitedForData) {
                // We wait for the user now
                p.setWaitedFor(previouslyWaitedFor.includes(p));
            }
        });
    }

    resetAllWaitedForData(): void {
        this.players.values.forEach(p => p.resetWaitedFor());
    }

    checkWaitedForPlayers(): Player[] {
        const waitedUsers = this.leafState.getWaitedUsers();
        const notWaitedForAnymore: Player[] = [];
        this.players.values.forEach(p => {
            if (!p.waitedForData || p.waitedForData.handled) {
                // We are either still waiting for the user or  we are in a state like
                // PlaceOrders, ChooseHouseCards or Bidding, which allows changing the decision.
                // To make it totally perfect we would need to add the possibilty to delete
                // the last sent value and send a new one. But for now we just use the
                // response time value of the first user message that made him not-waited-for anymore.
                return;
            }

            if (!waitedUsers.includes(p.user) || p.waitedForData.leafStateId != this.entireGame.leafStateId) {
                // We don't wait for the user anymore, send their personal response time to the website
                p.sendPbemResponseTime();
                notWaitedForAnymore.push(p);
            }
        });

        return notWaitedForAnymore;
    }

    createVote(initiator: User, type: VoteType): Vote {
        const vote = new Vote(this, v4(), this.players.values.map(p => p.house), initiator, type);
        vote.type.onVoteCreated(vote);

        this.votes.set(vote.id, vote);

        this.entireGame.broadcastToClients({
            type: "vote-started",
            vote: vote.serializeToClient(false, null)
        });

        this.entireGame.notifyUsers(_.without(this.players.keys, initiator), NotificationType.NEW_VOTE_STARTED);

        return vote;
    }

    getControllerOfHouse(house: House): Player {
        if (this.isVassalHouse(house)) {
            const suzerainHouse = this.game.vassalRelations.tryGet(house, null);

            if (suzerainHouse == null) {
                throw new Error(`getControllerOfHouse(${house.name}) failed as there is no suzerainHouse`);
            }

            return this.getControllerOfHouse(suzerainHouse);
        } else {
            const player = this.players.values.find(p => p.house == house);

            if (player == null) {
                throw new Error(`getControllerOfHouse(${house.name}) failed due to a fatal error`);
            }

            return player;
        }
    }

    getNextInTurnOrder(house: House | null, except: House | null = null): House {
        const turnOrder = this.game.getTurnOrder();

        if (house == null) {
            return turnOrder[0];
        }

        const i = turnOrder.indexOf(house);

        const nextHouse = turnOrder[(i + 1) % turnOrder.length];

        if (nextHouse == except) {
            return this.getNextInTurnOrder(nextHouse);
        }

        return nextHouse;
    }

    getNextNonVassalInTurnOrder(house: House | null): House {
        house = this.getNextInTurnOrder(house);

        if (!this.isVassalHouse(house)) {
            return house;
        } else {
            return this.getNextNonVassalInTurnOrder(house);
        }
    }

    changePowerTokens(house: House, delta: number): number {
        if (this.isVassalHouse(house)) {
            return 0;
        }

        const originalValue = house.powerTokens;

        const powerTokensOnBoardCount = this.game.countPowerTokensOnBoard(house);
        const maxPowerTokenCount = house.maxPowerTokens - powerTokensOnBoardCount;

        house.powerTokens += delta;
        house.powerTokens = Math.max(0, Math.min(house.powerTokens, maxPowerTokenCount));

        this.entireGame.broadcastToClients({
            type: "change-power-token",
            houseId: house.id,
            powerTokenCount: house.powerTokens
        });

        return house.powerTokens - originalValue;
    }

    transformUnits(region: Region, units: Unit[], targetType: UnitType): Unit[] {
        this.broadcastRemoveUnits(region, units, false);

        const transformed = units.map(unit => {
            unit.region.units.delete(unit.id);

            const newUnit = this.game.createUnit(unit.region, targetType, unit.allegiance);
            newUnit.region.units.set(newUnit.id, newUnit);

            newUnit.wounded = unit.wounded;

            return newUnit;
        });

        this.entireGame.broadcastToClients({
            type: "add-units",
            regionId: region.id,
            units: transformed.map(u => u.serializeToClient()),
            isTransform: true
        });

        return transformed;
    }

    broadcastRemoveUnits(region: Region, units: Unit[], animate = true): void {
        this.entireGame.broadcastToClients({
            type: "remove-units",
            regionId: region.id,
            unitIds: units.map(u => u.id),
            animate: animate
        });
    }

    checkVictoryConditions(): boolean {
        if (this.game.areVictoryConditionsFulfilled()) {
            // Game is finished
            this.setChildGameState(new GameEndedGameState(this)).firstStart(this.game.getPotentialWinner());
            return true;
        } else {
            return false;
        }
    }

    onPlayerClockTimeout(player: Player): void {
        // Use a try catch here as an exception in a timer callback seems to crash the server
        let updateLastActive = false;

        try {
            if (!player.liveClockData) {
                throw new Error("LiveClockData must be present in onPlayerClockTimeout");
            }

            this.endPlayerClock(player, false);

            if (this.players.size == 2) {
                // Replacing a vassal now could lead to an invalid state.
                // E.G. PayDebtsGameState will fail because there is no-one left to do the destroy units choice
                // When we are in combat, replacing vassal will fail, as there is no house left to assign the new vassal
                // Therefore we go to GameEnded first and then replace the last house with a vassal:

                const winner = _.without(this.players.values, player)[0].house;
                this.setChildGameState(new GameEndedGameState(this)).firstStart(winner);
                updateLastActive = true;
                this.entireGame.checkGameStateChanged();
            }

            if (this.hasChildGameState(ThematicDraftHouseCardsGameState) || this.hasChildGameState(DraftHouseCardsGameState)) {
                // Determine winner by finding the one with the most time left. On draw apply normal tie breaker.
                const winner = _.orderBy(this.game.getPotentialWinners().filter(h => h != player.house && !this.isVassalHouse(h)),
                    h => this.getControllerOfHouse(h).liveClockData?.remainingSeconds, "desc")[0];

                this.setChildGameState(new GameEndedGameState(this)).firstStart(winner);
                updateLastActive = true;
                return;
            }

            this.replacePlayerByVassal(player, ReplacementReason.CLOCK_TIMEOUT);
        } catch (e) {
            const message = typeof e === "string"
                ? e
                : e instanceof Error
                    ? e.message
                    : "Unknown error in onPlayerClockTimeout";
            console.error(message);
            this.entireGame.onCaptureSentryMessage(`onPlayerClockTimeout failed for user ${player.user.name} (${player.user.id}): ${message}`, "fatal");
        } finally {
            this.entireGame.checkGameStateChanged();
            this.entireGame.doPlayerClocksHandling();
            this.entireGame.saveGame(updateLastActive);
        }
    }

    endPlayerClock(player: Player, clearTimer = true): void {
        if (!player.liveClockData) {
            return;
        }

        if (clearTimer && player.liveClockData.serverTimer) {
            clearTimeout(player.liveClockData.serverTimer);
        }

        player.liveClockData.serverTimer = null;
        player.liveClockData.timerStartedAt = null;
        player.liveClockData.remainingSeconds = 0;

        this.entireGame.broadcastToClients({
            type: "stop-player-clock",
            remainingSeconds: 0,
            userId: player.user.id
        });
    }

    resumeGame(byVote = false): void {
        try {
            if (!this.paused) {
                throw new Error("Game must be paused here");
            }

            const pauseTimeInSeconds = getElapsedSeconds(this.paused);
            this.paused = null;
            this.willBeAutoResumedAt = null;
            this.autoResumeTimeout = null;

            // Cancel possible ResumeGame votes
            this.votes.values.filter(v => v.type instanceof ResumeGame && v.state == VoteState.ONGOING).forEach(v => {
                v.cancelVote();
            });

            this.log({
                type: "game-resumed",
                pauseTimeInSeconds: pauseTimeInSeconds,
                autoResumed: !byVote
            });
            this.entireGame.broadcastToClients({
                type: "game-resumed"
            });

            this.entireGame.doPlayerClocksHandling();
            this.entireGame.saveGame(false);
        } catch (e) {
            const message = typeof e === "string"
                ? e
                : e instanceof Error
                    ? e.message
                    : "Unknown error in resumeGame";
            console.error(message);
            this.entireGame.onCaptureSentryMessage(`resumeGame failed: ${message}`, "fatal");
        }
    }

    applyAverageOfRemainingClocksToNewPlayer(newPlayer: Player, oldPlayer: Player | null): void {
        if (!this.entireGame.gameSettings.onlyLive) {
            return;
        }

        const otherPlayers = this.players.values;
        _.pull(otherPlayers, newPlayer, oldPlayer);

        const avg = Math.floor(_.sum(otherPlayers.map(p => p.totalRemainingSeconds)) / otherPlayers.length);
        newPlayer.liveClockData = {
            remainingSeconds: avg,
            serverTimer: null,
            timerStartedAt: null
        }
    }

    replacePlayerByVassal(player: Player, reason: ReplacementReason): void {
        this.cancelPendingReplaceVotes();

        const newVassalHouse = player.house;

        // In case the new vassal house is needed for another vote, vote with Reject
        const missingVotes = this.votes.values.filter(v => v.state == VoteState.ONGOING && v.participatingHouses.includes(newVassalHouse) && !v.votes.has(newVassalHouse));
        missingVotes.forEach(v => {
            v.votes.set(newVassalHouse, false);
            this.entireGame.broadcastToClients({
                type: "vote-done",
                choice: false,
                vote: v.id,
                voter: newVassalHouse.id
            });

            // We don't need to call v.checkVoteFinished() here as we vote with Reject and therefore never call executeAccepted()
        });

        const forbiddenCommanders: House[] = [];
        // If we are in combat we can't assign the vassal to the opponent
        const anyCombat = this.getFirstChildGameState(CombatGameState);
        if (anyCombat) {
            const combat = anyCombat as CombatGameState;
            if (combat.isCommandingHouseInCombat(newVassalHouse)) {
                const commandedHouse = combat.getCommandedHouseInCombat(newVassalHouse);
                const enemy = combat.getEnemy(commandedHouse);

                forbiddenCommanders.push(this.getControllerOfHouse(enemy).house);
            }
        }

        if (reason == ReplacementReason.VOTE && !this.oldPlayerIds.includes(player.user.id)) {
            this.oldPlayerIds.push(player.user.id);
        } else if (reason == ReplacementReason.CLOCK_TIMEOUT && !this.timeoutPlayerIds.includes(player.user.id)) {
            this.timeoutPlayerIds.push(player.user.id);
        }
        // Delete the old player so the house is a vassal now
        this.players.delete(player.user);

        // Find new commander beginning with last in turn order
        let newCommander: House | null = null;
        for (const house of [...this.game.getTurnOrder()].reverse().filter(h => !this.isVassalHouse(h))) {
            if (!forbiddenCommanders.includes(house)) {
                newCommander = house;
                break;
            }
        }

        if (!newCommander) {
            throw new Error("Unable to determine new commander");
        }

        // It may happen that you replace a player which commands vassals. Assign them to the potential winner.
        this.game.vassalRelations.entries.forEach(([vassal, commander]) => {
            if (newVassalHouse == commander) {
                this.game.vassalRelations.set(vassal, newCommander as House);
            }
        });

        // Assign new commander to replaced house
        this.game.vassalRelations.set(newVassalHouse, newCommander);

        // Broadcast new vassal relations before deletion of player!
        this.broadcastVassalRelations();

        newVassalHouse.hasBeenReplacedByVassal = true;

        this.entireGame.broadcastToClients({
            type: "player-replaced",
            oldUser: player.user.id
        });

        this.log({
            type: "player-replaced",
            oldUser: player.user.id,
            house: newVassalHouse.id,
            reason: reason
        });

        // Save the house cards, so vassalization can be undone and cards can be re-assigned to a new player
        this.game.oldPlayerHouseCards.set(newVassalHouse, newVassalHouse.houseCards);
        this.entireGame.broadcastToClients({
            type: "update-old-player-house-cards",
            houseCards: this.game.oldPlayerHouseCards.entries.map(([h, hcs]) => [h.id, hcs.values.map(hc => hc.serializeToClient())])
        });

        // In case we are in combat we will do proceedHouseCardHandling() where we eventually recycle the deck,
        // then save the oldPlayerHouseCards again and then remove the house cards from this vassal house.
        if (!this.hasChildGameState(CombatGameState)) {
            // If we're not in combat, we have to remove the house cards from the new vassal now
            newVassalHouse.houseCards = new BetterMap();

            this.entireGame.broadcastToClients({
                type: "update-house-cards",
                house: newVassalHouse.id,
                houseCards: []
            });
        }

        // Perform action of current state
        this.leafState.actionAfterVassalReplacement(newVassalHouse);

        // In case the new vassal should execute a wildlings effect, skip it
        if (this.hasChildGameState(WildlingCardEffectInTurnOrderGameState)) {
            const wildlingEffect = this.getChildGameState(WildlingCardEffectInTurnOrderGameState) as WildlingCardEffectInTurnOrderGameState<GameState<any, any>>;
            const leaf = this.leafState as any;
            if (leaf.house && leaf.house == newVassalHouse) {
                wildlingEffect.proceedNextHouse(newVassalHouse);
            }
        }

        const newCommanderPlayer = this.players.values.find(p => p.house == newCommander);
        // If we are waiting for the new commander, notify them about their turn
        if (newCommanderPlayer && this.leafState.getWaitedUsers().includes(newCommanderPlayer.user)) {
            this.entireGame.notifyWaitedUsers([newCommanderPlayer.user]);
        }
    }

    onServerMessage(message: ServerMessage): void {
        if (message.type == "supply-adjusted") {
            const supplies: [House, number][] = message.supplies.map(([houseId, supply]) => [this.game.houses.get(houseId), supply]);

            supplies.forEach(([house, supply]) => house.supplyLevel = supply);
        } else if (message.type == "change-control-power-token") {
            const region = this.world.regions.get(message.regionId);
            const house = message.houseId ? this.game.houses.get(message.houseId) : null;

            region.controlPowerToken = house;
        } else if (message.type == "change-wildling-strength") {
            this.game.wildlingStrength = message.wildlingStrength;
        } else if (message.type == "add-units") {
            const region = this.world.regions.get(message.regionId);
            const units = message.units.map(u => {
                const unit = Unit.deserializeFromServer(this.game, u);
                unit.region = region;
                region.units.set(unit.id, unit);
                return unit;
            });

            units.forEach(u =>
                this.unitsToBeAnimated.set(u, {
                    highlight: {active: true, color: message.isTransform ? "yellow": "green"},
                    animateAttention: message.isTransform,
                    animateFadeIn: !message.isTransform
                }));
            window.setTimeout(() => units.forEach(u => this.unitsToBeAnimated.delete(u)), 4000);
        } else if (message.type == "change-garrison") {
            const region = this.world.regions.get(message.region);

            region.garrison = message.newGarrison;
        } else if (message.type == "remove-units") {
            const region = this.world.regions.get(message.regionId);
            const units = message.unitIds.map(uid => region.units.get(uid));

            if (message.animate) {
                units.forEach(u =>
                    this.unitsToBeAnimated.set(u, {
                        highlight: {active: true, color: "red"},
                        animateFadeOut: true
                    }));
                window.setTimeout(() => units.forEach(unit => {
                    region.units.delete(unit.id);
                    this.unitsToBeAnimated.delete(unit);
                }), 4000);
            } else {
                units.forEach(unit => region.units.delete(unit.id));
            }
        } else if (message.type == "change-state-house-card") {
            const house = this.game.houses.get(message.houseId);
            const cards = message.cardIds.map(cid => house.houseCards.get(cid));

            cards.forEach(hc => hc.state = message.state);
        } else if (message.type == "move-units") {
            const from = this.world.regions.get(message.from);
            const to = this.world.regions.get(message.to);
            const units = message.units.map(uid => from.units.get(uid));

            if (from != to) {
                units.forEach(u => {
                    this.marchMarkers.set(u, to);
                });
            }

            window.setTimeout(() => {
                units.forEach(u => {
                    this.marchMarkers.tryDelete(u);
                    from.units.delete(u.id);
                    to.units.set(u.id, u);
                    u.region = to;
                });
            }, message.isRetreat ? 4500 : 5000);
        } else if (message.type == "units-wounded") {
            const region = this.world.regions.get(message.regionId);
            const units = message.unitIds.map(uid => region.units.get(uid));

            units.forEach(u => u.wounded = true);
        } else if (message.type == "change-power-token") {
            const house = this.game.houses.get(message.houseId);

            house.powerTokens = message.powerTokenCount;
        } else if (message.type == "new-turn") {
            this.game.turn++;
            this.game.valyrianSteelBladeUsed = false;
            this.world.regions.forEach(r => r.units.forEach(u => u.wounded = false));
        } else if (message.type == "add-game-log") {
            this.gameLogManager.logs.push({data: message.data, time: new Date(message.time * 1000), resolvedAutomatically: message.resolvedAutomatically});
            if (this.onLogReceived) {
                this.onLogReceived(message.data);
            }
        } else if (message.type == "change-tracker") {
            const newOrder = message.tracker.map(hid => this.game.houses.get(hid));

            if (message.trackerI == 0) {
                this.game.ironThroneTrack = newOrder;
            } else if (message.trackerI == 1) {
                this.game.fiefdomsTrack = newOrder;
            } else if (message.trackerI == 2) {
                this.game.kingsCourtTrack = newOrder;
            }
        } else if (message.type == "update-westeros-decks") {
            this.game.westerosDecks = message.westerosDecks.map(wd => wd.map(wc => WesterosCard.deserializeFromServer(wc)));
            this.game.winterIsComingHappened = message.winterIsComingHappened;
        } else if (message.type == "hide-top-wildling-card") {
            this.game.houses.forEach(h => h.knowsNextWildlingCard = false);
            this.game.clientNextWildlingCardId = null;
        } else if (message.type == "reveal-top-wildling-card") {
            this.game.houses.get(message.houseId).knowsNextWildlingCard = true;
            this.game.clientNextWildlingCardId = message.cardId;
        } else if (message.type == "vote-started") {
            const vote = Vote.deserializeFromServer(this, message.vote);
            this.votes.set(vote.id, vote);
            if (this.onVoteStarted) {
                this.onVoteStarted();
            }
        } else if (message.type == "vote-cancelled") {
            const vote = this.votes.get(message.vote);
            vote.cancelled = true;
        } else if (message.type == "vote-done") {
            const vote = this.votes.get(message.vote);
            const voter = this.game.houses.get(message.voter);

            vote.votes.set(voter, message.choice);
        } else if (message.type == "player-replaced") {
            const oldPlayer = this.players.get(this.entireGame.users.get(message.oldUser));
            const newUser = message.newUser ? this.entireGame.users.get(message.newUser) : null;
            const newPlayer = newUser ? new Player(newUser, oldPlayer.house) : null;

            if (newPlayer && message.liveClockRemainingSeconds !== undefined) {
                newPlayer.liveClockData = {
                    remainingSeconds: message.liveClockRemainingSeconds,
                    serverTimer: null,
                    timerStartedAt: null
                }
            }

            if (newUser && newPlayer) {
                this.players.set(newUser, newPlayer);
            } else {
                oldPlayer.house.hasBeenReplacedByVassal = true;
            }

            this.players.delete(oldPlayer.user);

            this.forceRerender();
        } else if (message.type == "vassal-replaced") {
            const house = this.game.houses.get(message.house);
            house.hasBeenReplacedByVassal = false;
            const user = this.entireGame.users.get(message.user);
            const newPlayer = new Player(user, house);

            if (message.liveClockRemainingSeconds !== undefined) {
                newPlayer.liveClockData = {
                    remainingSeconds: message.liveClockRemainingSeconds,
                    serverTimer: null,
                    timerStartedAt: null
                }
            }

            this.players.set(user, newPlayer);

            this.forceRerender();
        } else if (message.type == "vassal-relations") {
            this.game.vassalRelations = new BetterMap(message.vassalRelations.map(([vId, cId]) => [this.game.houses.get(vId), this.game.houses.get(cId)]));
            this.forceRerender();
        } else if (message.type == "update-house-cards") {
            const house = this.game.houses.get(message.house);
            house.houseCards = new BetterMap(message.houseCards.map(hc => [hc.id, HouseCard.deserializeFromServer(hc)]));
        } else if (message.type == "later-house-cards-applied") {
            const house = this.game.houses.get(message.house);
            house.laterHouseCards = null;
        } else if (message.type == "update-house-cards-for-drafting") {
            this.game.houseCardsForDrafting = new BetterMap(message.houseCards.map(hc => [hc.id, HouseCard.deserializeFromServer(hc)]));
        } else if (message.type == "update-deleted-house-cards") {
            this.game.deletedHouseCards = new BetterMap(message.houseCards.map(hc => [hc.id, HouseCard.deserializeFromServer(hc)]));
        } else if (message.type == "update-old-player-house-cards") {
            this.game.oldPlayerHouseCards = new BetterMap(message.houseCards.map(([hid, hcs]) => [this.game.houses.get(hid), new BetterMap(hcs.map(hc => [hc.id, HouseCard.deserializeFromServer(hc)]))]));
        } else if (message.type == "update-max-turns") {
            this.game.maxTurns = message.maxTurns;
        } else if (message.type == "loyalty-token-gained") {
            const house = this.game.houses.get(message.house);
            const region = this.world.regions.get(message.region);
            house.gainedLoyaltyTokens = message.newLoyaltyTokenCount;
            region.loyaltyTokens = 0;
        } else if (message.type == "loyalty-token-placed") {
            const region = this.world.regions.get(message.region);
            region.loyaltyTokens = message.newLoyaltyTokenCount;
        } else if (message.type == "dragon-strength-token-removed") {
            _.pull(this.game.dragonStrengthTokens, message.fromRound);
            this.game.removedDragonStrengthToken = message.fromRound;
        } else if (message.type == "update-loan-cards") {
            this.game.theIronBank.loanCardDeck = message.loanCardDeck.map(lc => LoanCard.deserializeFromServer(this.game, lc));
            this.game.theIronBank.purchasedLoans = message.purchasedLoans.map(lc => LoanCard.deserializeFromServer(this.game, lc));
            this.game.theIronBank.loanSlots = message.loanSlots.map(lc => lc ? LoanCard.deserializeFromServer(this.game, lc) : null);
        } else if (message.type == "update-region-modifiers") {
            const region = this.game.world.regions.get(message.region);

            if (message.castleModifier) {
                region.castleModifier = message.castleModifier;
            }
            if (message.barrelModifier) {
                region.barrelModifier = message.barrelModifier;
            }
            if (message.crownModifier) {
                region.crownModifier = message.crownModifier;
            }
        } else if (message.type == "update-completed-objectives") {
            message.objectives.forEach(([hid, objectives]) => {
                this.game.houses.get(hid).completedObjectives = objectives.map(ocid => objectiveCards.get(ocid));
            });

            message.victoryPointCount.forEach(([hid, vpc]) => {
                this.game.houses.get(hid).victoryPoints = vpc;
            });
        } else if (message.type == "update-secret-objectives") {
            this.game.houses.get(message.house).secretObjectives = message.objectives.map(ocid => objectiveCards.get(ocid));
        } else if (message.type == "update-usurper") {
            this.game.usurper = message.house ? this.game.houses.get(message.house) : null;
        } else if (message.type == "start-player-clock") {
            const player = this.players.get(this.entireGame.users.get(message.userId));

            if (!player.liveClockData) {
                throw new Error("LiveClockData must be present in start-player-clock");
            }

            player.liveClockData.remainingSeconds = message.remainingSeconds;
            player.liveClockData.timerStartedAt = new Date(message.timerStartedAt);
        } else if (message.type == "stop-player-clock") {
            const player = this.players.get(this.entireGame.users.get(message.userId));

            if (!player.liveClockData) {
                throw new Error("LiveClockData must be present stop-player-clock");
            }

            player.liveClockData.remainingSeconds = message.remainingSeconds;
            player.liveClockData.timerStartedAt = null;
        } else if (message.type == "game-paused") {
            this.paused = new Date();
            if (message.willBeAutoResumedAt) {
                this.willBeAutoResumedAt = new Date(message.willBeAutoResumedAt);
            }
        } else if (message.type == "game-resumed") {
            this.paused = null;
            this.willBeAutoResumedAt = null;
        } else if (message.type == "preemptive-raid-new-attack" && this.onPreemptiveRaidNewAttack) {
            // Todo: Handle this in WildlingAttackGameState
            const biddings = message.biddings.map(([bid, hids]) =>
                [bid, hids.map(hid => this.game.houses.get(hid))] as [number, House[]]);
            const highestBidder = this.game.houses.get(message.highestBidder);
            this.onPreemptiveRaidNewAttack(biddings, highestBidder);
        } else if (message.type == "houses-swapped") {
            const initiator = this.players.get(this.entireGame.users.get(message.initiator));
            const swappingPlayer = this.players.get(this.entireGame.users.get(message.swappingUser));

            const swappingHouse = swappingPlayer.house;
            swappingPlayer.house = initiator.house;
            initiator.house = swappingHouse;
            this.forceRerender();
        } else if (message.type == "reveal-orders") {
            message.orders.forEach(([rid, _oid]) => {
                const r = this.world.regions.get(rid);
                this.ordersToBeAnimated.set(r, {animateFlip: true })
            });
            window.setTimeout(() => {
                this.ordersOnBoard = new BetterMap(message.orders.map(([rid, oid]) => {
                    const r = this.world.regions.get(rid);
                    this.ordersToBeAnimated.delete(r);
                    return [r, orders.get(oid)];
                }));
            }, 1200);
        } else if (message.type == "remove-orders") {
            message.regions.map(rid => this.world.regions.get(rid)).forEach(r => {
                this.ordersOnBoard.delete(r);
            });
        }
        else {
            this.childGameState.onServerMessage(message);
        }
    }

    forceRerender(): void {
        if (this.rerender > 0) {
            this.rerender--;
        } else {
            this.rerender++;
        }
    }

    launchCancelGameVote(): void {
        if (window.confirm('Do you want to launch a vote to cancel the game?')) {
            this.entireGame.sendMessageToServer({
                type: "launch-cancel-game-vote"
            });
        }
    }

    launchEndGameVote(): void {
        if (window.confirm('Do you want to launch a vote to end the game after the current round?')) {
            this.entireGame.sendMessageToServer({
                type: "launch-end-game-vote"
            });
        }
    }

    launchPauseGameVote(): void {
        if (window.confirm('Do you want to launch a vote to pause the game?')) {
            this.entireGame.sendMessageToServer({
                type: "launch-pause-game-vote"
            });
        }
    }

    launchResumeGameVote(): void {
        if (window.confirm('Do you want to launch a vote to resume the game?')) {
            this.entireGame.sendMessageToServer({
                type: "launch-resume-game-vote"
            });
        }
    }

    launchExtendPlayerClocksVote(): void {
        if (window.confirm('Do you want to launch a vote to extend all player clocks by 15 minutes?')) {
            this.entireGame.sendMessageToServer({
                type: "launch-extend-player-clocks-vote"
            });
        }
    }

    canLaunchCancelGameVote(player: Player | null): {result: boolean; reason: string} {
        if (this.entireGame.gameSettings.tournamentMode) {
            return {result: false, reason: "forbidden-in-tournament-mode"};
        }

        if (this.paused) {
            return {result: false, reason: "game-paused"};
        }

        const existingVotes = this.votes.values.filter(v => v.state == VoteState.ONGOING && v.type instanceof CancelGame);

        if (existingVotes.length > 0) {
            return {result: false, reason: "already-existing"};
        }

        if (player == null || !this.players.values.includes(player)) {
            return {result: false, reason: "only-players-can-vote"};
        }

        if (this.isCancelled) {
            return {result: false, reason: "already-cancelled"};
        }

        if (this.isEnded) {
            return {result: false, reason: "already-ended"};
        }

        return {result: true, reason: ""};
    }

    canLaunchEndGameVote(player: Player | null): {result: boolean; reason: string} {
        if (this.entireGame.gameSettings.tournamentMode) {
            return {result: false, reason: "forbidden-in-tournament-mode"};
        }

        if (this.paused) {
            return {result: false, reason: "game-paused"};
        }

        const existingVotes = this.votes.values.filter(v => v.state == VoteState.ONGOING && v.type instanceof EndGame);

        if (existingVotes.length > 0) {
            return {result: false, reason: "already-existing"};
        }

        if (player == null || !this.players.values.includes(player)) {
            return {result: false, reason: "only-players-can-vote"};
        }

        if (this.isCancelled) {
            return {result: false, reason: "already-cancelled"};
        }

        if (this.isEnded) {
            return {result: false, reason: "already-ended"};
        }

        if (this.game.turn == this.game.maxTurns) {
            return {result: false, reason: "already-last-turn"};
        }

        return {result: true, reason: ""};
    }

    canLaunchPauseGameVote(player: Player | null): {result: boolean; reason: string} {
        if (!this.entireGame.gameSettings.onlyLive) {
            return {result: false, reason: "no-live-clock-game"};
        }

        const existingVotes = this.votes.values.filter(v => v.state == VoteState.ONGOING && v.type instanceof PauseGame);

        if (existingVotes.length > 0) {
            return {result: false, reason: "already-existing"};
        }

        if (this.paused) {
            return {result: false, reason: "already-paused"};
        }

        if (player == null || !this.players.values.includes(player)) {
            return {result: false, reason: "only-players-can-vote"};
        }

        if (this.isCancelled) {
            return {result: false, reason: "already-cancelled"};
        }

        if (this.isEnded) {
            return {result: false, reason: "already-ended"};
        }

        return {result: true, reason: ""};
    }

    canLaunchResumeGameVote(player: Player | null): {result: boolean; reason: string} {
        if (!this.entireGame.gameSettings.onlyLive) {
            return {result: false, reason: "no-live-clock-game"};
        }

        const existingVotes = this.votes.values.filter(v => v.state == VoteState.ONGOING && v.type instanceof ResumeGame);

        if (existingVotes.length > 0) {
            return {result: false, reason: "already-existing"};
        }

        if (!this.paused) {
            return {result: false, reason: "not-paused"};
        }

        if (player == null || !this.players.values.includes(player)) {
            return {result: false, reason: "only-players-can-vote"};
        }

        if (this.isCancelled) {
            return {result: false, reason: "already-cancelled"};
        }

        if (this.isEnded) {
            return {result: false, reason: "already-ended"};
        }

        return {result: true, reason: ""};
    }

    canLaunchExtendPlayerClocksVote(player: Player | null): {result: boolean; reason: string} {
        if (this.entireGame.gameSettings.tournamentMode) {
            return {result: false, reason: "forbidden-in-tournament-mode"};
        }

        if (this.paused) {
            return {result: false, reason: "game-paused"};
        }

        if (!this.entireGame.gameSettings.onlyLive) {
            return {result: false, reason: "no-live-clock-game"};
        }

        const existingVotes = this.votes.values.filter(v => v.state == VoteState.ONGOING && v.type instanceof ExtendPlayerClocks);

        if (existingVotes.length > 0) {
            return {result: false, reason: "already-existing"};
        }

        const acceptedVotes = this.votes.values.filter(v => v.state == VoteState.ACCEPTED && v.type instanceof ExtendPlayerClocks);

        const allowedVotesCount = this.game.turn == this.game.maxTurns ? 2 : 1;
        if (acceptedVotes.length >= allowedVotesCount) {
            return {result: false, reason: "already-extended"};
        }

        const refusedVotes = this.votes.values.filter(v => v.state == VoteState.REFUSED && v.type instanceof ExtendPlayerClocks);
        if (this.game.turn != this.game.maxTurns && refusedVotes.length > 1) {
            return {result: false, reason: "max-vote-count-reached"};
        }

        if (player == null || !this.players.values.includes(player)) {
            return {result: false, reason: "only-players-can-vote"};
        }

        if (this.isCancelled) {
            return {result: false, reason: "already-cancelled"};
        }

        if (this.isEnded) {
            return {result: false, reason: "already-ended"};
        }

        return {result: true, reason: ""};
    }

    canLaunchReplacePlayerVote(fromUser: User | null, replaceWithVassal = false, forHouse: House | null = null): {result: boolean; reason: string} {
        if (this.entireGame.gameSettings.tournamentMode && !replaceWithVassal) {
            return {result: false, reason: "forbidden-in-tournament-mode"};
        }

        if (this.paused) {
            return {result: false, reason: "game-paused"};
        }

        if (!fromUser) {
            return {result: false, reason: "only-authenticated-users-can-vote"};
        }

        if (!replaceWithVassal && this.players.keys.includes(fromUser)) {
            return {result: false, reason: "already-playing"};
        }

        if (replaceWithVassal) {
            if (!this.players.keys.includes(fromUser)) {
                return {result: false, reason: "only-players-can-vote"};
            }

            if (this.players.get(fromUser).house == forHouse) {
                return {result: false, reason: "vassalizing-yourself-is-forbidden"};
            }

            if (!this.entireGame.gameSettings.onlyLive && this.players.size == this.entireGame.minPlayerCount) {
                return {result: false, reason: "min-player-count-reached"};
            }

            if (this.childGameState instanceof DraftHouseCardsGameState) {
                return {result: false, reason: "ongoing-house-card-drafting"}
            }

            if (this.childGameState instanceof ThematicDraftHouseCardsGameState) {
                return {result: false, reason: "ongoing-house-card-drafting"}
            }
        }

        const existingVotes = this.votes.values.filter(v => v.state == VoteState.ONGOING && ((!replaceWithVassal && v.type instanceof ReplacePlayer) || v.type instanceof ReplacePlayerByVassal));
        if (existingVotes.length > 0) {
            return {result: false, reason: "ongoing-vote"};
        }

        if (this.isCancelled) {
            return {result: false, reason: "game-cancelled"};
        }

        if (this.isEnded) {
            return {result: false, reason: "game-ended"};
        }

        return {result: true, reason: ""};
    }

    canLaunchSwapHousesVote(initiator: User | null, swappingPlayer: Player): {result: boolean; reason: string} {
        if (this.entireGame.gameSettings.tournamentMode) {
            return {result: false, reason: "forbidden-in-tournament-mode"};
        }

        if (!initiator || !this.players.keys.includes(initiator)) {
            return {result: false, reason: "only-players-can-vote"};
        }

        if (initiator == swappingPlayer.user) {
            return {result: false, reason: "cannot-swap-with-yourself"};
        }

        const player = this.players.get(initiator);

        if (this.entireGame.isFeastForCrows) {
            if (this.game.turn != 0) {
                return {result: false, reason: "secret-objectives-chosen"};
            }

            if (this.hasChildGameState(ChooseInitialObjectivesGameState)) {
                const chooseInitialObjectives = this.getChildGameState(ChooseInitialObjectivesGameState) as ChooseInitialObjectivesGameState;
                if (chooseInitialObjectives.childGameState.readyHouses.keys.some(h => player.house == h || swappingPlayer.house == h)) {
                    return {result: false, reason: "secret-objectives-chosen"};
                }
            }
        }

        const existingVotes = this.votes.values.filter(v => v.state == VoteState.ONGOING && v.type instanceof SwapHouses);

        if (existingVotes.length > 0) {
            return {result: false, reason: "ongoing-vote"};
        }

        if (this.isCancelled) {
            return {result: false, reason: "game-cancelled"};
        }

        if (this.isEnded) {
            return {result: false, reason: "game-ended"};
        }

        return {result: true, reason: ""};
    }

    canLaunchReplaceVassalVote(fromUser: User | null, forHouse: House): {result: boolean; reason: string} {
        if (this.entireGame.gameSettings.tournamentMode) {
            return {result: false, reason: "forbidden-in-tournament-mode"};
        }

        if (this.entireGame.gameSettings.onlyLive) {
            return {result: false, reason: "forbidden-in-clock-games"};
        }

        if (this.paused) {
            return {result: false, reason: "game-paused"};
        }

        if (!fromUser) {
            return {result: false, reason: "only-authenticated-users-can-vote"};
        }

        if (this.players.keys.includes(fromUser)) {
            return {result: false, reason: "already-playing"};
        }

        if (this.players.values.some(p => p.house == forHouse)) {
            return {result: false, reason: "not-a-vassal"};
        }

        if (!forHouse.hasBeenReplacedByVassal) {
            return {result: false, reason: "not-a-replaced-vassal"};
        }

        const existingVotes = this.votes.values.filter(v => v.state == VoteState.ONGOING && v.type instanceof ReplaceVassalByPlayer);
        if (existingVotes.length > 0) {
            return {result: false, reason: "ongoing-vote"};
        }

        if (this.isCancelled) {
            return {result: false, reason: "game-cancelled"};
        }

        if (this.isEnded) {
            return {result: false, reason: "game-ended"};
        }

        return {result: true, reason: ""};
    }

    isHouseDefeated(house: House | null): boolean {
        if (!house) {
            return true;
        }

        // A house is considered defeated when it has no castle areas and no land units anymore
        return this.world.regions.values.filter(r => r.castleLevel > 0 && r.getController() == house).length == 0 &&
            this.world.getUnitsOfHouse(house).filter(u => u.type.id != "ship").length == 0;
    }

    launchReplacePlayerVote(player: Player): void {
        this.entireGame.sendMessageToServer({
            type: "launch-replace-player-vote",
            player: player.user.id
        });
    }

    launchReplacePlayerByVassalVote(player: Player): void {
        this.entireGame.sendMessageToServer({
            type: "launch-replace-player-by-vassal-vote",
            player: player.user.id
        });
    }

    launchReplaceVassalByPlayerVote(house: House): void {
        this.entireGame.sendMessageToServer({
            type: "launch-replace-vassal-by-player-vote",
            house: house.id
        });
    }

    launchSwapHousesVote(player: Player): void {
        this.entireGame.sendMessageToServer({
            type: "launch-swap-houses-vote",
            swappingUser: player.user.id
        });
    }

    getVassalHouses(): House[] {
        return this.game.houses.values.filter(h => this.isVassalHouse(h));
    }

    isVassalControlledByPlayer(vassal: House, player: Player): boolean {
        if (!this.isVassalHouse(vassal)) {
            throw new Error();
        }

        return this.game.vassalRelations.tryGet(vassal, null) == player.house;
    }

    getVassalsControlledByPlayer(player: Player): House[] {
        return this.getVassalHouses().filter(h => this.isVassalControlledByPlayer(h, player));
    }

    getControlledHouses(player: Player): House[] {
        const houses  = this.getVassalsControlledByPlayer(player);
        houses.unshift(player.house);
        return houses;
    }

    getNonClaimedVassalHouses(): House[] {
        return this.getVassalHouses().filter(v => !this.game.vassalRelations.has(v));
    }

    isVassalHouse(house: House): boolean {
        return !this.players.values.map(p => p.house).includes(house);
    }

    // Returns (House | null) to support .includes(region.getController())
    // but can safely be casted to House[]
    getOtherVassalFamilyHouses(house: House): (House | null)[] {
        const result: House[] = [];
        if (this.game.vassalRelations.has(house)) {
            // If house is a vassal add its commander ...
            const vassalCommader = this.game.vassalRelations.get(house);
            result.push(vassalCommader);

            // ... and all other vassals except myself
            this.game.vassalRelations.entries.forEach(([vassal, commander]) => {
                if (commander == vassalCommader && vassal != house) {
                    result.push(vassal);
                }
            });
        } else {
            // If house is no vassal add potentially controlled vassals
            this.game.vassalRelations.entries.forEach(([vassal, commander]) => {
                if (commander == house) {
                    result.push(vassal);
                }
            });
        }

        return result;
    }

    getTurnOrderWithoutVassals(): House[] {
        return this.game.getTurnOrder().filter(h => !this.isVassalHouse(h));
    }

    broadcastObjectives(): void {
        this.entireGame.broadcastToClients({
            type: "update-completed-objectives",
            objectives: this.game.houses.values.map(h => [h.id, h.completedObjectives.map(oc => oc.id)] as [string, string[]]),
            victoryPointCount: this.game.houses.values.map(h => [h.id, h.victoryPoints])
        });

        this.players.values.forEach(p => {
            p.user.send({
                type: "update-secret-objectives",
                house: p.house.id,
                objectives: p.house.secretObjectives.map(oc => oc.id)
            });
        });
    }

    broadcastVassalRelations(): void {
        this.entireGame.broadcastToClients({
            type: "vassal-relations",
            vassalRelations: this.game.vassalRelations.entries.map(([vassal, commander]) => [vassal.id, commander.id])
        });
    }

    broadcastWesterosDecks(): void {
        this.entireGame.broadcastToClients({
            type: "update-westeros-decks",
            westerosDecks: this.game.westerosDecks.map(wd => wd.slice(0, this.game.revealedWesterosCards)
                .concat(shuffleInPlace(wd.slice(this.game.revealedWesterosCards))).map(wc => wc.serializeToClient())),
            winterIsComingHappened: this.game.winterIsComingHappened
        });
    }

    canGiftPowerTokens(house: House): boolean {
        if (!this.entireGame.gameSettings.allowGiftingPowerTokens) {
            // Targaryen always must be able to gift their tokens, so they can leave the game when defeated
            if (house != this.game.targaryen) {
                return false;
            }
        }

        if (this.entireGame.hasChildGameState(CombatGameState) &&
            !(this.entireGame.leafState instanceof DeclareSupportGameState)) {
            return false;
        }

        return !this.isVassalHouse(house);
    }

    serializeToClient(admin: boolean, user: User | null): SerializedIngameGameState {
        // If user == null, then the game state needs to be serialized
        // in an "admin" version (i.e. containing all data).
        // Otherwise, provide a serialized version that hides data
        // based on which user is requesting the data.
        const player: Player | null = user
            ? (this.players.has(user)
                ? this.players.get(user)
                : null)
            : null;

        return {
            type: "ingame",
            players: this.players.values.map(p => p.serializeToClient()),
            oldPlayerIds: this.oldPlayerIds,
            replacerIds: this.replacerIds,
            timeoutPlayerIds: this.timeoutPlayerIds,
            game: this.game.serializeToClient(admin, player),
            gameLogManager: this.gameLogManager.serializeToClient(admin, user),
            ordersOnBoard: this.ordersOnBoard.mapOver(r => r.id, o => o.id),
            votes: this.votes.values.map(v => v.serializeToClient(admin, player)),
            paused: this.paused ? this.paused.getTime() : null,
            willBeAutoResumedAt: this.willBeAutoResumedAt ? this.willBeAutoResumedAt.getTime() : null,
            childGameState: this.childGameState.serializeToClient(admin, player)
        };
    }

    static deserializeFromServer(entireGame: EntireGame, data: SerializedIngameGameState): IngameGameState {
        const ingameGameState = new IngameGameState(entireGame);

        ingameGameState.game = Game.deserializeFromServer(ingameGameState, data.game);
        ingameGameState.players = new BetterMap(
            data.players.map(p => [entireGame.users.get(p.userId), Player.deserializeFromServer(ingameGameState, p)])
        );
        ingameGameState.oldPlayerIds = data.oldPlayerIds;
        ingameGameState.replacerIds = data.replacerIds;
        ingameGameState.timeoutPlayerIds = data.timeoutPlayerIds;
        ingameGameState.votes = new BetterMap(data.votes.map(sv => [sv.id, Vote.deserializeFromServer(ingameGameState, sv)]));
        ingameGameState.ordersOnBoard = new BetterMap(
            data.ordersOnBoard.map(([regionId, orderId]) => (
                [ingameGameState.world.regions.get(regionId), orders.get(orderId)]
            ))
        );
        ingameGameState.gameLogManager = GameLogManager.deserializeFromServer(ingameGameState, data.gameLogManager);
        ingameGameState.paused = data.paused ? new Date(data.paused) : null;
        ingameGameState.willBeAutoResumedAt = data.willBeAutoResumedAt ? new Date(data.willBeAutoResumedAt) : null;
        ingameGameState.childGameState = ingameGameState.deserializeChildGameState(data.childGameState);

        return ingameGameState;
    }

    deserializeChildGameState(data: SerializedIngameGameState["childGameState"]): IngameGameState["childGameState"] {
        switch (data.type) {
            case "westeros":
                return WesterosGameState.deserializeFromServer(this, data);
            case "planning":
                return PlanningGameState.deserializeFromServer(this, data);
            case "action":
                return ActionGameState.deserializeFromServer(this, data);
            case "game-ended":
                return GameEndedGameState.deserializeFromServer(this, data);
            case "cancelled":
                return CancelledGameState.deserializeFromServer(this, data);
            case "draft-house-cards":
                return DraftHouseCardsGameState.deserializeFromServer(this, data);
            case "thematic-draft-house-cards":
                return ThematicDraftHouseCardsGameState.deserializeFromServer(this, data);
            case "pay-debts":
                return PayDebtsGameState.deserializeFromServer(this, data);
            case "choose-initial-objectives":
                return ChooseInitialObjectivesGameState.deserializeFromServer(this, data);
        }
    }
}

export interface SerializedIngameGameState {
    type: "ingame";
    players: SerializedPlayer[];
    oldPlayerIds: string[];
    replacerIds: string[];
    timeoutPlayerIds: string[];
    game: SerializedGame;
    votes: SerializedVote[];
    gameLogManager: SerializedGameLogManager;
    ordersOnBoard: [string, number][];
    paused: number | null;
    willBeAutoResumedAt: number | null;
    childGameState: SerializedPlanningGameState | SerializedActionGameState | SerializedWesterosGameState
        | SerializedGameEndedGameState | SerializedCancelledGameState | SerializedDraftHouseCardsGameState
        | SerializedThematicDraftHouseCardsGameState | SerializedPayDebtsGameState
        | SerializedChooseInitialObjectivesGameState;
}
