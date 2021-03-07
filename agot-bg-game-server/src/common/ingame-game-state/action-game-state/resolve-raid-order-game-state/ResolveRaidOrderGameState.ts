import GameState from "../../../GameState";
import ActionGameState from "../ActionGameState";
import ResolveSingleRaidOrderGameState, {SerializedResolveSingleRaidOrderGameState} from "./resolve-single-raid-order-game-state/ResolveSingleRaidOrderGameState";
import IngameGameState from "../../IngameGameState";
import EntireGame from "../../../EntireGame";
import Game from "../../game-data-structure/Game";
import World from "../../game-data-structure/World";
import House from "../../game-data-structure/House";
import Player from "../../Player";
import {ClientMessage} from "../../../../messages/ClientMessage";
import {ServerMessage} from "../../../../messages/ServerMessage";
import Region from "../../game-data-structure/Region";
import RaidOrderType from "../../game-data-structure/order-types/RaidOrderType";
import RaidSupportOrderType from "../../game-data-structure/order-types/RaidSupportOrderType";
import { noRaidOrder } from "../../game-data-structure/westeros-card/planning-restriction/planningRestrictions";

export default class ResolveRaidOrderGameState extends GameState<ActionGameState, ResolveSingleRaidOrderGameState> {
    resolvedRaidSupportOrderRegions: Region[] = [];

    get actionGameState(): ActionGameState {
        return this.parentGameState;
    }

    get ingameGameState(): IngameGameState {
        return this.actionGameState.ingameGameState;
    }

    get entireGame(): EntireGame {
        return this.actionGameState.entireGame;
    }

    get game(): Game {
        return this.ingameGameState.game;
    }

    get world(): World {
        return this.game.world;
    }

    firstStart(): void {
        if (this.actionGameState.planningRestrictions.some(pr => pr == noRaidOrder)) {
            // This will disallow a RaidOrSupportPlusOne Order to be executed
            this.actionGameState.onResolveRaidOrderGameStateFinish();
        };

        this.ingameGameState.log({
            type: "action-phase-resolve-raid-began"
        });

        this.proceedNextResolveSingleRaidOrder(null);
    }

    onPlayerMessage(player: Player, message: ClientMessage): void {
        this.childGameState.onPlayerMessage(player, message);
    }

    onServerMessage(message: ServerMessage): void {
        this.childGameState.onServerMessage(message);

    }

    onResolveSingleRaidOrderGameStateEnd(house: House): void {
        // Check if an other raid order can be resolved
        this.proceedNextResolveSingleRaidOrder(house);
    }

    proceedNextResolveSingleRaidOrder(lastHouseToResolve: House | null = null): void {
        const houseToResolve = this.getNextHouseToResolveRaidOrder(lastHouseToResolve);

        if (houseToResolve == null) {
            // All raid orders have been executed
            // go the to the next phase
            this.actionGameState.onResolveRaidOrderGameStateFinish();
            return;
        }

        this.setChildGameState(new ResolveSingleRaidOrderGameState(this)).firstStart(houseToResolve);
    }

    getNextHouseToResolveRaidOrder(lastHouseToResolve: House | null): House | null {
        let currentHouseToCheck = lastHouseToResolve ? this.ingameGameState.getNextInTurnOrder(lastHouseToResolve) : this.game.getTurnOrder()[0];

        // Check each house in order to find one that has an available March order.
        // Check at most once for each house
        for (let i = 0;i < this.game.houses.size;i++) {
            if (this.getRegionsWithRaidOrderOfHouse(currentHouseToCheck).length > 0) {
                return currentHouseToCheck;
            }

            currentHouseToCheck = this.ingameGameState.getNextInTurnOrder(currentHouseToCheck);
        }

        // If no house has any raid order available, return null
        return null;
    }

    getRegionsWithRaidOrderOfHouse(house: House): [Region, RaidOrderType | RaidSupportOrderType][] {
        return this.actionGameState.getRegionsWithRaidOrderOfHouse(house).filter(([r, _ot]) => !this.resolvedRaidSupportOrderRegions.includes(r));
    }

    serializeToClient(admin: boolean, player: Player | null): SerializedResolveRaidOrderGameState {
        return {
            type: "resolve-raid-order",
            childGameState: this.childGameState.serializeToClient(admin, player),
            resolvedRaidSupportOrderRegions: this.resolvedRaidSupportOrderRegions.map(r => r.id)
        };
    }

    static deserializeFromServer(actionGameState: ActionGameState, data: SerializedResolveRaidOrderGameState): ResolveRaidOrderGameState {
        const resolveRaidOrder = new ResolveRaidOrderGameState(actionGameState);

        resolveRaidOrder.childGameState = resolveRaidOrder.deserializeChildGameState(data.childGameState);
        resolveRaidOrder.resolvedRaidSupportOrderRegions = data.resolvedRaidSupportOrderRegions.map(rid => actionGameState.game.world.regions.get(rid))

        return resolveRaidOrder;
    }

    deserializeChildGameState(data: SerializedResolveRaidOrderGameState["childGameState"]): ResolveSingleRaidOrderGameState {
        return ResolveSingleRaidOrderGameState.deserializeFromServer(this, data);
    }
}

export interface SerializedResolveRaidOrderGameState {
    type: "resolve-raid-order";
    childGameState: SerializedResolveSingleRaidOrderGameState;
    resolvedRaidSupportOrderRegions: string[];
}
