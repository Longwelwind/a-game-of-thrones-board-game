import GameState from "../../../../GameState";
import UseRavenGameState from "../UseRavenGameState";
import Player from "../../../Player";
import {ClientMessage, SeeTopWildlingCardAction} from "../../../../../messages/ClientMessage";
import {ServerMessage} from "../../../../../messages/ServerMessage";
import IngameGameState from "../../../IngameGameState";
import EntireGame from "../../../../EntireGame";
import House from "../../../game-data-structure/House";
import WildlingCard from "../../../game-data-structure/wildling-card/WildlingCard";

export default class SeeTopWildlingCardGameState extends GameState<UseRavenGameState> {
    // Will be null client-side for players who can't see the card
    topWildlingCard: WildlingCard | null;

    get useRavenGameState(): UseRavenGameState {
        return this.parentGameState;
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

    firstStart() {
        this.topWildlingCard = this.useRavenGameState.game.wildlingDeck[0];
    }

    onPlayerMessage(player: Player, message: ClientMessage) {
        if (message.type == "choose-top-wildling-card-action") {
            if (this.ravenHolder != player.house) {
                return;
            }

            if (message.action == SeeTopWildlingCardAction.PUT_AT_BOTTOM) {
                const removedCard = this.useRavenGameState.game.wildlingDeck.shift() as WildlingCard;

                this.useRavenGameState.game.wildlingDeck.push(removedCard);
            }

            this.useRavenGameState.onSeeTopWildlingCardGameStateEnd();
        }
    }

    choose(action: SeeTopWildlingCardAction) {
        this.entireGame.sendMessageToServer({
            type: "choose-top-wildling-card-action",
            action: action
        });
    }

    onServerMessage(message: ServerMessage) {

    }

    getPhaseName(): string {
        return "See top wildling card";
    }

    serializeToClient(admin: boolean, player: Player | null): SerializedSeeTopWildlingCardGameState {
        return {
            type: "see-top-wildling-card",
            topWildlingCard: this.topWildlingCard
                ? (admin || (player && player.house == this.ravenHolder)
                    ? this.topWildlingCard.id
                    : null)
                : null
        };
    }

    static deserializeFromServer(useRavenGameState: UseRavenGameState, data: SerializedSeeTopWildlingCardGameState): SeeTopWildlingCardGameState {
        const seeTopWildlingCard = new SeeTopWildlingCardGameState(useRavenGameState);

        seeTopWildlingCard.topWildlingCard = data.topWildlingCard ? useRavenGameState.game.wildlingDeck.find(c => c.id == data.topWildlingCard) as WildlingCard : null;

        return seeTopWildlingCard;
    }
}

export interface SerializedSeeTopWildlingCardGameState {
    type: "see-top-wildling-card";
    topWildlingCard: number | null;
}
