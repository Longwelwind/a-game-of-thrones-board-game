import GameState from "../../../../GameState";
import UseRavenGameState from "../UseRavenGameState";
import Player from "../../../Player";
import {ClientMessage} from "../../../../../messages/ClientMessage";
import {ServerMessage} from "../../../../../messages/ServerMessage";
import IngameGameState from "../../../IngameGameState";
import EntireGame from "../../../../EntireGame";
import House from "../../../game-data-structure/House";
import orders from "../../../game-data-structure/orders";
import ActionGameState from "../../ActionGameState";
import Region from "../../../game-data-structure/Region";
import Order from "../../../game-data-structure/Order";
import BetterMap from "../../../../../utils/BetterMap";

export default class ReplaceOrderGameState extends GameState<UseRavenGameState> {
    get useRavenGameState(): UseRavenGameState {
        return this.parentGameState;
    }

    get actionGameState(): ActionGameState {
        return this.useRavenGameState.actionGameState;
    }

    get ingameGameState(): IngameGameState {
        return this.useRavenGameState.ingameGameState;
    }

    get entireGame(): EntireGame {
        return this.useRavenGameState.entireGame;
    }

    get ravenHolder(): House {
        return this.useRavenGameState.ravenHolder;
    }

    onPlayerMessage(player: Player, message: ClientMessage): void {
        if (message.type == "replace-order") {
            if (player.house != this.ravenHolder) {
                return;
            }

            const order = orders.get(message.orderId);
            const region = this.ingameGameState.game.world.regions.get(message.regionId);

            if (region.getController() != player.house) {
                return;
            }


            if (!this.actionGameState.ordersOnBoard.has(region)) {
                return
            }

            const replacedOrder = this.actionGameState.ordersOnBoard.get(region);

            if (!this.getAvailableOrders(replacedOrder).includes(order)) {
                return;
            }

            this.actionGameState.ordersOnBoard.set(region, order);

            this.entireGame.log(
                `The holder of the Raven Token (${this.ravenHolder.name}) changed his **${replacedOrder.type.name}**`,
                ` into a **${order.type.name}** in **${region.name}**`
            );

            this.entireGame.broadcastToClients({
                type: "raven-order-replaced",
                regionId: region.id,
                orderId: order.id
            });

            this.useRavenGameState.onReplaceOrderGameStateEnd();
        } else if (message.type == "skip-replace-order") {
            this.useRavenGameState.onReplaceOrderGameStateEnd();
        }
    }

    getAvailableOrders(replacedOrder: Order): Order[] {
        const placedOrders = new BetterMap(
            this.actionGameState.getOrdersOfHouse(this.ravenHolder).filter(([r, o]) => replacedOrder != o)
        );

        return this.ingameGameState.game.getAvailableOrders(placedOrders, this.ravenHolder);
    }

    replaceOrder(region: Region, order: Order) {
        this.entireGame.sendMessageToServer({
            type: "replace-order",
            regionId: region.id,
            orderId: order.id
        });
    }

    skip() {
        this.entireGame.sendMessageToServer({
            type: "skip-replace-order"
        });
    }

    onServerMessage(message: ServerMessage) {
        if (message.type == "raven-order-replaced") {
            const region = this.ingameGameState.game.world.regions.get(message.regionId);
            const order = orders.get(message.orderId);

            this.actionGameState.ordersOnBoard.set(region, order);
        }
    }

    getPhaseName(): string {
        return "Replace order";
    }

    serializeToClient(admin: boolean, player: Player | null): SerializedReplaceOrderGameState {
        return {
            type: "replace-order"
        };
    }

    static deserializeFromServer(useRavenGameState: UseRavenGameState, data: SerializedReplaceOrderGameState): ReplaceOrderGameState {
        return new ReplaceOrderGameState(useRavenGameState);
    }
}

export interface SerializedReplaceOrderGameState {
    type: "replace-order";
}
