import GameState from "../../../GameState";
import House from "../../game-data-structure/House";
import {ClientMessage} from "../../../../messages/ClientMessage";
import Player from "../../Player";
import {ServerMessage} from "../../../../messages/ServerMessage";
import Game from "../../game-data-structure/Game";
import * as _ from "lodash";
import {observable} from "mobx";
import BetterMap from "../../../../utils/BetterMap";
import User from "../../../../server/User";
import IngameGameState from "../../IngameGameState";

export interface BiddingGameStateParent extends GameState<any, any> {
    game: Game;
    ingame: IngameGameState;
    onBiddingGameStateEnd: (results: [number, House[]][]) => void;
}

export default class BiddingGameState<ParentGameState extends BiddingGameStateParent> extends GameState<ParentGameState> {
    participatingHouses: House[];
    // Client-side, this structure will only contain -1 as value.
    @observable bids: BetterMap<House, number> = new BetterMap<House, number>();

    get game(): Game {
        return this.parentGameState.game;
    }

    onPlayerMessage(player: Player, message: ClientMessage): void {
        if (message.type == "bid") {
            this.setBid(player.house, message.powerTokens);
        }
    }

    private setBid(house: House, value: number): void {
        if (!this.participatingHouses.includes(house)) {
            return;
        }

        const bid = Math.max(0, Math.min(value, house.powerTokens));
        this.bids.set(house, bid);

        this.entireGame.broadcastToClients({
            type: "bid-done",
            houseId: house.id
        });

        this.checkAndProceedEndOfBidding();
    }

    checkAndProceedEndOfBidding(): void {
        if (this.getHousesLeftToBid().length > 0) {
            return;
        }

        // Remove the power tokens
        this.bids.entries.forEach(([house, bid]) => {
            house.powerTokens -= bid;

            this.entireGame.broadcastToClients({
                type: "change-power-token",
                houseId: house.id,
                powerTokenCount: house.powerTokens
            });
        });

        // Create a convenient array containing the results
        const housesPerBid = new BetterMap<number, House[]>();
        this.bids.forEach((bid, house) => {
            if (housesPerBid.has(bid)) {
                housesPerBid.get(bid).push(house);
            } else {
                housesPerBid.set(bid, [house]);
            }
        });

        const results = _.sortBy(housesPerBid.entries, ([bid, _]) => -bid);

        this.parentGameState.onBiddingGameStateEnd(results);
    }

    onServerMessage(message: ServerMessage): void {
        if (message.type == "bid-done") {
            const house = this.game.houses.get(message.houseId);
            this.bids.set(house, -1);
        }
    }

    bid(powerTokens: number): void {
        this.entireGame.sendMessageToServer({
            type: "bid",
            powerTokens: powerTokens
        });
    }

    hasBid(house: House): boolean {
        return this.bids.has(house);
    }

    getWaitedUsers(): User[] {
        return this.getHousesLeftToBid().map(h => this.parentGameState.ingame.getControllerOfHouse(h).user);
    }

    getHousesLeftToBid(): House[] {
        return _.difference(this.participatingHouses, this.bids.keys);
    }

    getPhaseName(): string {
        return "Bidding phase";
    }

    firstStart(participatingHouses: House[] = []): void {
        this.participatingHouses = participatingHouses;
        // Houses with no Power Tokens automatically bid 0
        this.participatingHouses.filter(h => h.powerTokens == 0).forEach(h => {
            this.setBid(h, 0);
        });
    }

    serializeToClient(admin: boolean, player: Player | null): SerializedBiddingGameState {
        return {
            type: "bidding",
            participatingHouses: this.participatingHouses.map(h => h.id),
            bids: this.bids.entries.map(([house, bid]) => {
                // If a player requested the serialized version, only give his own bid.
                // If admin, give all bid.
                if (admin || (player && house == player.house)) {
                    return [house.id, bid];
                } else {
                    return [house.id, -1];
                }
            })
        };
    }

    static deserializeFromServer<ParentGameState extends BiddingGameStateParent>(parent: ParentGameState, data: SerializedBiddingGameState): BiddingGameState<ParentGameState> {
        const biddingGameState = new BiddingGameState(parent);

        biddingGameState.participatingHouses = data.participatingHouses.map(hid => parent.game.houses.get(hid));
        biddingGameState.bids = new BetterMap(data.bids.map(([houseId, bid]) => [parent.game.houses.get(houseId), bid]));

        return biddingGameState
    }
}

export interface SerializedBiddingGameState {
    type: "bidding";
    participatingHouses: string[];
    bids: [string, number][];
}
