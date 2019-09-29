import GameState from "../../../GameState";
import WesterosGameState from "../WesterosGameState";
import PlayerReconcileArmiesGameState, {SerializedPlayerReconcileArmiesGameState} from "./player-reconcile-armies-game-state/PlayerReconcileArmiesGameState";
import House from "../../game-data-structure/House";
import Game from "../../game-data-structure/Game";
import {ClientMessage} from "../../../../messages/ClientMessage";
import Player from "../../Player";
import {ServerMessage} from "../../../../messages/ServerMessage";

type Trigger = {onReconcileArmiesGameStateEnd(westerosGameState: WesterosGameState): void};

export default class ReconcileArmiesGameState extends GameState<WesterosGameState, PlayerReconcileArmiesGameState> {
    trigger: Trigger;

    get westerosGameState(): WesterosGameState {
        return this.parentGameState;
    }

    get game(): Game {
        return this.westerosGameState.game;
    }

    firstStart(trigger: Trigger): void {
        this.trigger = trigger;

        this.proceedReconcileArmies(null);
    }

    onPlayerMessage(player: Player, message: ClientMessage): void {
        this.childGameState.onPlayerMessage(player, message);
    }

    onServerMessage(message: ServerMessage) {
        this.childGameState.onServerMessage(message);
    }

    proceedReconcileArmies(lastReconciled: House | null) {
        if (lastReconciled == this.game.ironThroneTrack[this.game.ironThroneTrack.length - 1]) {
            this.trigger.onReconcileArmiesGameStateEnd(this.westerosGameState);
            return;
        }

        const nextHouseToReconcile = lastReconciled ? this.game.getNextInTurnOrder(lastReconciled) : this.game.ironThroneTrack[0];

        // Check if this house needs to reconcile armies
        if (this.game.hasTooMuchArmies(nextHouseToReconcile)) {
            this.setChildGameState(new PlayerReconcileArmiesGameState(this)).firstStart(nextHouseToReconcile);
        } else {
            this.proceedReconcileArmies(nextHouseToReconcile);
        }
    }

    onPlayerReconcileArmiesGameStateEnd(house: House) {
        this.proceedReconcileArmies(house);
    }

    serializeToClient(): SerializedReconcileArmiesGameState {
        return {
            type: "reconcile-armies",
            childGameState: this.childGameState.serializeToClient()
        };
    }

    static deserializeFromServer(westeros: WesterosGameState, data: SerializedReconcileArmiesGameState): ReconcileArmiesGameState {
        const reconcileArmies = new ReconcileArmiesGameState(westeros);
        reconcileArmies.childGameState = reconcileArmies.deserializeChildGameState(data.childGameState);
        return reconcileArmies;
    }

    deserializeChildGameState(data: SerializedPlayerReconcileArmiesGameState): PlayerReconcileArmiesGameState {
        return PlayerReconcileArmiesGameState.deserializeFromServer(this, data);
    }
}

export interface SerializedReconcileArmiesGameState {
    type: "reconcile-armies";
    childGameState: SerializedPlayerReconcileArmiesGameState;
}
