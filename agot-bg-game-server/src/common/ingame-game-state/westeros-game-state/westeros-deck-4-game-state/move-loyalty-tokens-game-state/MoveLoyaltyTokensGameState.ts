import GameState from "../../../../GameState";
import SimpleChoiceGameState, {SerializedSimpleChoiceGameState} from "../../../simple-choice-game-state/SimpleChoiceGameState";
import Game from "../../../game-data-structure/Game";
import Player from "../../../Player";
import {ClientMessage} from "../../../../../messages/ClientMessage";
import {ServerMessage} from "../../../../../messages/ServerMessage";
import IngameGameState from "../../../IngameGameState";
import House from "../../../game-data-structure/House";
import Region from "../../../game-data-structure/Region";
import WesterosDeck4GameState from "../WesterosDeck4GameState";
import WesterosGameState from "../../WesterosGameState";
import ResolveMoveLoyaltyTokenGameState, { SerializedResolveMoveLoyaltyTokenGameState } from "./resolve-move-loyalty-token-game-state/ResolveMoveLoyaltyTokenGameState";
import { land } from "../../../../../common/ingame-game-state/game-data-structure/regionTypes";
import { observable } from "mobx";

interface PreviousMovement {
    house: House;
    from: Region;
    to: Region;
}

export default class MoveLoyaltyTokensGameState extends GameState<WesterosDeck4GameState, SimpleChoiceGameState | ResolveMoveLoyaltyTokenGameState> {
    resolveOrder: House[];
    costsToCancelPreviousMovement: number;
    previousMovement: PreviousMovement | null;
    @observable
    acceptAllMovements: boolean;

    get game(): Game {
        return this.parentGameState.game;
    }

    get ingame(): IngameGameState {
        return this.parentGameState.ingame;
    }

    get westeros(): WesterosGameState {
        return this.parentGameState.parentGameState;
    }

    get validFromRegions(): Region[] {
        return this.game.world.regions.values.filter(r => r.loyaltyTokens > 0).filter(r => this.getValidTargetRegions(r).length > 0);
    }

    get house(): House {
        return this.resolveOrder[0];
    }

    firstStart(resolveOrder: House[], costsToCancelPreviousMovement: number, previousMovement: PreviousMovement | null): void {
        this.resolveOrder = resolveOrder;
        this.costsToCancelPreviousMovement = costsToCancelPreviousMovement;
        this.previousMovement = previousMovement;
        this.proceedNextResolve();
    }

    proceedNextResolve(): void {
        this.ingame.gainLoyaltyTokens();

        if (this.ingame.checkVictoryConditions()) {
            return;
        }

        const nextHouse = this.pullNextHouseToResolve();
        if (!nextHouse) {
            this.westeros.onWesterosCardEnd();
            return;
        }

        this.setChildGameState(new ResolveMoveLoyaltyTokenGameState(this)).firstStart(nextHouse);
    }

    pullNextHouseToResolve(): House | undefined {
        if (this.resolveOrder.length == 0 || this.validFromRegions.length == 0) {
            return undefined;
        }

        return this.resolveOrder.shift();
    }

    getValidTargetRegions(regionFrom: Region): Region[] {
        return this.game.world.getNeighbouringRegions(regionFrom).filter(r => r.type == land);
    }

    getChoices(house: House): string[] {
        const result: string[] = [];
        result.push("Ignore");
        if (house.powerTokens < this.costsToCancelPreviousMovement) {
            return result;
        }

        result.push(`Discard ${this.costsToCancelPreviousMovement} Power token${this.costsToCancelPreviousMovement != 1 ? "s" : ""} to cancel the previous movement`);
        return result;
    }

    onSimpleChoiceGameStateEnd(choice: number, resolvedAutomatically: boolean): void {
        if (!this.game.targaryen) {
            throw new Error("Targaryen must be available here!");
        }

        const house = this.game.targaryen;

        if (choice == 0) {
            this.ingame.log({
                type: "move-loyalty-token-choice",
                house: house.id,
                powerTokensDiscardedToCancelMovement: 0
            }, resolvedAutomatically);
        } else if (choice == 1) {
            this.ingame.log({
                type: "move-loyalty-token-choice",
                house: house.id,
                powerTokensDiscardedToCancelMovement: this.costsToCancelPreviousMovement
            });

            if (this.previousMovement == null) {
                throw new Error();
            }

            // Remove the Power token
            this.ingame.changePowerTokens(house, -this.costsToCancelPreviousMovement);

            // Undo the last move
            this.moveLoyaltyToken(this.previousMovement.to, this.previousMovement.from);
        }

        this.previousMovement = null;
        this.proceedNextResolve();
    }

    moveLoyaltyToken(regionFrom: Region, regionTo: Region): void {
        if (regionFrom.loyaltyTokens < 1) {
            throw new Error("Tried to move a loyalty token which doesn't exist!");
        }

        regionFrom.loyaltyTokens -= 1;
        regionTo.loyaltyTokens += 1;

        this.entireGame.broadcastToClients({
            type: "loyalty-token-placed",
            region: regionFrom.id,
            newLoyaltyTokenCount: regionFrom.loyaltyTokens
        });

        this.entireGame.broadcastToClients({
            type: "loyalty-token-placed",
            region: regionTo.id,
            newLoyaltyTokenCount: regionTo.loyaltyTokens
        });
    }

    setChooseCancelLastMoveGameState(_houseWhichMovedLoyaltyTokens: House): void {
        if (!this.game.targaryen) {
            throw new Error("Targaryen must be available here!");
        }

        if (!this.previousMovement) {
            throw new Error("Previous movement must be set here");
        }

        this.setChildGameState(new SimpleChoiceGameState(this)).firstStart(this.game.targaryen,
            `House Targaryen may discard ${this.costsToCancelPreviousMovement} Power token${this.costsToCancelPreviousMovement != 1 ? "s" : ""} to move the loyalty\xa0token from ${this.previousMovement.to.name} back to ${this.previousMovement.from.name}.`,
            this.getChoices(this.game.targaryen));
    }

    sendAcceptAllMovements(newValue: boolean): void {
        this.entireGame.sendMessageToServer({
            type: "change-accept-all-loyalty-token-movements",
            newValue: newValue
        });
    }

    onPlayerMessage(player: Player, message: ClientMessage): void {
        if (!this.game.targaryen) {
            throw new Error("Targaryen must be available here!");
        }

        if (message.type == "change-accept-all-loyalty-token-movements" && this.ingame.getControllerOfHouse(this.game.targaryen) == player) {
            this.acceptAllMovements = message.newValue;
            player.user.send({
                type: "accept-all-loyalty-token-movements-changed",
                newValue: this.acceptAllMovements
            });
        } else {
            this.childGameState.onPlayerMessage(player, message);
        }
    }

    onServerMessage(message: ServerMessage): void {
        if (message.type == "accept-all-loyalty-token-movements-changed") {
            this.acceptAllMovements = message.newValue;
        } else {
            this.childGameState.onServerMessage(message);
        }
    }

    serializeToClient(admin: boolean, player: Player | null): SerializedMoveLoyaltyTokensGameState {
        const playerControlsTargaryen = this.game.targaryen && this.ingame.getControllerOfHouse(this.game.targaryen) == player;
        return {
            type: "move-loyalty-tokens",
            resolveOrder: this.resolveOrder.map(h => h.id),
            costsToCancelPreviousMovement: this.costsToCancelPreviousMovement,
            previousMovement: this.previousMovement ? {
                house: this.previousMovement.house.id,
                from: this.previousMovement.from.id,
                to: this.previousMovement.to.id
            } : null,
            acceptAllMovements: admin || playerControlsTargaryen ? this.acceptAllMovements : false,
            childGameState: this.childGameState.serializeToClient(admin, player) ?? null
        };
    }

    static deserializeFromServer(westerosDeck4: WesterosDeck4GameState, data: SerializedMoveLoyaltyTokensGameState): MoveLoyaltyTokensGameState {
        const gameState = new MoveLoyaltyTokensGameState(westerosDeck4);

        gameState.resolveOrder = data.resolveOrder.map(hid => westerosDeck4.game.houses.get(hid));
        gameState.costsToCancelPreviousMovement = data.costsToCancelPreviousMovement;
        gameState.previousMovement = data.previousMovement ? {
            house: westerosDeck4.game.houses.get(data.previousMovement.house),
            from: westerosDeck4.game.world.regions.get(data.previousMovement.from),
            to: westerosDeck4.game.world.regions.get(data.previousMovement.to)
        } : null;
        gameState.acceptAllMovements = data.acceptAllMovements;
        gameState.childGameState = gameState.deserializeChildGameState(data.childGameState);

        return gameState;
    }

    deserializeChildGameState(data: SerializedMoveLoyaltyTokensGameState["childGameState"]): MoveLoyaltyTokensGameState["childGameState"] {
        if (data.type == "simple-choice") {
            return SimpleChoiceGameState.deserializeFromServer(this, data);
        } else if (data.type == "resolve-move-loyalty-token"){
            return ResolveMoveLoyaltyTokenGameState.deserializeFromServer(this, data);
        } else {
            throw new Error();
        }
    }
}

export interface SerializedMoveLoyaltyTokensGameState {
    type: "move-loyalty-tokens";
    resolveOrder: string[];
    costsToCancelPreviousMovement: number;
    previousMovement: {
        house: string;
        from: string;
        to: string;
    } | null;
    acceptAllMovements: boolean;
    childGameState: SerializedSimpleChoiceGameState | SerializedResolveMoveLoyaltyTokenGameState;
}
