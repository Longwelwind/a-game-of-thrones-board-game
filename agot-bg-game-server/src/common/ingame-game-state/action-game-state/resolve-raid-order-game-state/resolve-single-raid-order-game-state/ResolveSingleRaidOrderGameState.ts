import GameState from "../../../../GameState";
import ResolveRaidOrderGameState from "../ResolveRaidOrderGameState";
import House from "../../../game-data-structure/House";
import {ClientMessage} from "../../../../../messages/ClientMessage";
import Player from "../../../Player";
import {ServerMessage} from "../../../../../messages/ServerMessage";
import Region from "../../../game-data-structure/Region";
import EntireGame from "../../../../EntireGame";
import Game from "../../../game-data-structure/Game";
import World from "../../../game-data-structure/World";
import IngameGameState from "../../../IngameGameState";
import ActionGameState from "../../ActionGameState";
import RaidOrderType from "../../../game-data-structure/order-types/RaidOrderType";
import ConsolidatePowerOrderType from "../../../game-data-structure/order-types/ConsolidatePowerOrderType";
import {raid} from "../../../game-data-structure/order-types/orderTypes";

export default class ResolveSingleRaidOrderGameState extends GameState<ResolveRaidOrderGameState> {
    house: House;

    get resolveRaidOrderGameState(): ResolveRaidOrderGameState {
        return this.parentGameState;
    }

    get entireGame(): EntireGame {
        return this.resolveRaidOrderGameState.entireGame;
    }

    get actionGameState(): ActionGameState {
        return this.resolveRaidOrderGameState.actionGameState;
    }

    get ingameGameState(): IngameGameState {
        return this.resolveRaidOrderGameState.ingameGameState;
    }

    get game(): Game {
        return this.ingameGameState.game;
    }

    get world(): World {
        return this.game.world;
    }

    firstStart(house: House) {
        this.house = house;
    }

    onPlayerMessage(player: Player, message: ClientMessage) {
        if (message.type == "resolve-raid") {
            if (player.house != this.house) {
                return;
            }

            const orderRegion = this.world.regions.get(message.orderRegionId);
            const targetRegion = message.targetRegionId ? this.world.regions.get(message.targetRegionId) : null;

            if (orderRegion.getController() != this.house) {
                return;
            }

            if (!this.getRegionWithRaidOrders().includes(orderRegion)) {
                return;
            }

            const orderType = this.actionGameState.ordersOnBoard.get(orderRegion).type as RaidOrderType;

            if (targetRegion) {
                const orderTarget = this.actionGameState.ordersOnBoard.get(targetRegion);

                if (!this.getRaidableRegions(orderRegion, orderType).includes(targetRegion)) {
                    return;
                }

                // If the raided order is a consolidate power, transfer some power tokens
                const raidedHouse = targetRegion.getController();
                if (raidedHouse == null) {
                    // This should normally never happens as a region that has an order always have a controller
                    throw new Error();
                }

                if (orderTarget.type instanceof ConsolidatePowerOrderType) {
                    this.house.changePowerTokens(-2);
                    raidedHouse.changePowerTokens(2);

                    this.entireGame.broadcastToClients({
                        type: "change-power-token",
                        houseId: this.house.id,
                        powerTokenCount: this.house.powerTokens
                    });
                    this.entireGame.broadcastToClients({
                        type: "change-power-token",
                        houseId: raidedHouse.id,
                        powerTokenCount: raidedHouse.powerTokens
                    });
                }

                this.entireGame.log(
                    `${this.house.name} raids **${raidedHouse.name}**'s **${orderTarget.type.name}** order`,
                    ` from **${targetRegion.name}**`
                );

                this.actionGameState.ordersOnBoard.delete(targetRegion);
                this.entireGame.broadcastToClients({
                    type: "action-phase-change-order",
                    region: targetRegion.id,
                    order: null
                });
            } else {
                this.entireGame.log(
                    `**${this.house.name}** resolves a raid order at **${orderRegion.name}**`
                );
            }

            this.actionGameState.ordersOnBoard.delete(orderRegion);
            this.entireGame.broadcastToClients({
                type: "action-phase-change-order",
                region: orderRegion.id,
                order: null
            });

            this.resolveRaidOrderGameState.onResolveSingleRaidOrderGameStateEnd(this.house);
        }
    }

    getRegionWithRaidOrders(): Region[] {
        return this.actionGameState.getRegionsWithRaidOrderOfHouse(this.house);
    }

    onServerMessage(_message: ServerMessage): void {

    }

    getRaidableRegions(orderRegion: Region, raid: RaidOrderType): Region[] {
        return this.world.getNeighbouringRegions(orderRegion)
            .filter(r => this.actionGameState.ordersOnBoard.has(r))
            .filter(r => raid.isValidRaidableOrder(this.actionGameState.ordersOnBoard.get(r)))
            .filter(r => r.type.kind == orderRegion.type.kind || orderRegion.type.canAdditionalyRaid == r.type.kind);
    }

    resolveRaid(orderRegion: Region, targetRegion: Region | null): void {
        this.entireGame.sendMessageToServer({
            type: "resolve-raid",
            orderRegionId: orderRegion.id,
            targetRegionId: targetRegion ? targetRegion.id : null
        });
    }

    serializeToClient(_admin: boolean, _player: Player | null): SerializedResolveSingleRaidOrderGameState {
        return {
            houseId: this.house.id
        };
    }

    static deserializeFromServer(resolveRaidOrderGameState: ResolveRaidOrderGameState, data: SerializedResolveSingleRaidOrderGameState): ResolveSingleRaidOrderGameState {
        const resolveSingleRaidOrderGameState = new ResolveSingleRaidOrderGameState(resolveRaidOrderGameState);

        resolveSingleRaidOrderGameState.house = resolveRaidOrderGameState.game.houses.get(data.houseId);

        return resolveSingleRaidOrderGameState;
    }
}

export interface SerializedResolveSingleRaidOrderGameState {
    houseId: string;
}
