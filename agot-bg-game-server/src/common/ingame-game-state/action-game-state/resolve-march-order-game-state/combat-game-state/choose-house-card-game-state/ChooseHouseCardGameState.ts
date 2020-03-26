import GameState from "../../../../../GameState";
import CombatGameState from "../CombatGameState";
import {ClientMessage} from "../../../../../../messages/ClientMessage";
import Player from "../../../../Player";
import HouseCard, {HouseCardState} from "../../../../game-data-structure/house-card/HouseCard";
import EntireGame from "../../../../../EntireGame";
import {ServerMessage} from "../../../../../../messages/ServerMessage";
import {observable} from "mobx";
import House from "../../../../game-data-structure/House";
import BetterMap from "../../../../../../utils/BetterMap";
import IngameGameState from "../../../../IngameGameState";
import _ from "lodash";
import User from "../../../../../../server/User";

export default class ChooseHouseCardGameState extends GameState<CombatGameState> {
    // A null value for a value can be present client-side, it indicates
    // that a house card was chosen but it may not be shown to the player.
    @observable houseCards = new BetterMap<House, HouseCard | null>();

    get combatGameState(): CombatGameState {
        return this.parentGameState;
    }

    get entireGame(): EntireGame {
        return this.combatGameState.entireGame;
    }

    get ingameGameState(): IngameGameState {
        return this.combatGameState.ingameGameState;
    }

    firstStart(): void {
        // In case users just have one house card it can be selected automatically
        this.tryAutomaticallyChooseLastHouseCard(this.combatGameState.attacker);
        this.tryAutomaticallyChooseLastHouseCard(this.combatGameState.defender);

        this.checkAndProceedEndOfChooseHouseCardGameState();
    }

    onServerMessage(message: ServerMessage): void {
        if (message.type == "house-card-chosen") {
            const house = this.combatGameState.game.houses.get(message.houseId);

            this.houseCards.set(house, null);
        }
    }

    onPlayerMessage(player: Player, message: ClientMessage): void {
        if (message.type == "choose-house-card") {
            if (!this.combatGameState.houseCombatDatas.has(player.house)) {
                return;
            }

            const houseCard = player.house.houseCards.get(message.houseCardId);

            if (!this.getChoosableCards(player.house).includes(houseCard)) {
                return;
            }

            this.houseCards.set(player.house, houseCard);

            this.entireGame.broadcastToClients({
                type: "house-card-chosen",
                houseId: player.house.id
            });

            this.checkAndProceedEndOfChooseHouseCardGameState();
        }
    }

    getWaitingForHouses(): House[] {
        return _.difference(this.combatGameState.houseCombatDatas.keys, this.houseCards.keys);
    }

    getWaitedUsers(): User[] {
        return this.getWaitingForHouses().map(h => this.ingameGameState.getControllerOfHouse(h).user);
    }

    getChoosableCards(house: House): HouseCard[] {
        return house.houseCards.values.filter(hc => hc.state == HouseCardState.AVAILABLE);
    }

    chooseHouseCard(houseCard: HouseCard): void {
        this.entireGame.sendMessageToServer({
            type: "choose-house-card",
            houseCardId: houseCard.id
        });
    }

    getPhaseName(): string {
        return "Choose a general";
    }

    serializeToClient(_admin: boolean, _player: Player | null): SerializedChooseHouseCardGameState {
        return {
            type: "choose-house-card",
            houseCards: this.houseCards.map((h, hc) => [h.id, hc ? hc.id : null])
        };
    }

    private checkAndProceedEndOfChooseHouseCardGameState(): void {
        if (this.houseCards.size == 2) {
            this.houseCards.forEach((houseCard, house) => this.combatGameState.houseCombatDatas.get(house).houseCard = houseCard);

            // "this.combatGameState.attackingHouseCombatData.houseCard" and
            // "this.combatGameState.defendingHouseCombatData.houseCard" will always be non-null
            // since they have just been set before, thus the two "ts-ignore". They could be later set to null
            // because of Tyrion Lannister, for example.
            this.ingameGameState.log({
                type: "combat-house-card-chosen",
                houseCards: [
                    // @ts-ignore
                    [this.combatGameState.attacker.id, this.combatGameState.attackingHouseCombatData.houseCard.id],
                    // @ts-ignore
                    [this.combatGameState.defender.id, this.combatGameState.defendingHouseCombatData.houseCard.id]
                ]
            });

            this.entireGame.broadcastToClients({
                type: "change-combat-house-card",
                // Same here, the houseCards will always be non-null
                // @ts-ignore
                houseCardIds: this.combatGameState.houseCombatDatas.map((h, hcd) => [h.id, hcd.houseCard.id])
            });

            this.combatGameState.onChooseHouseCardGameStateEnd();
        }
    }

    private tryAutomaticallyChooseLastHouseCard(house: House): void {
        const choosableCards = this.getChoosableCards(house);
        if (choosableCards.length == 1) {
            this.houseCards.set(house, choosableCards[0]);
        }
    }

    static deserializeFromServer(combatGameState: CombatGameState, data: SerializedChooseHouseCardGameState): ChooseHouseCardGameState {
        const chooseHouseCardGameState = new ChooseHouseCardGameState(combatGameState);

        chooseHouseCardGameState.houseCards = new BetterMap(data.houseCards.map(([hid, hcid]) => [
            combatGameState.game.houses.get(hid),
            hcid ? combatGameState.game.houses.get(hid).houseCards.get(hcid) : null
        ]));

        return chooseHouseCardGameState;
    }
}

export interface SerializedChooseHouseCardGameState {
    type: "choose-house-card";
    houseCards: [string, string | null][];
}
