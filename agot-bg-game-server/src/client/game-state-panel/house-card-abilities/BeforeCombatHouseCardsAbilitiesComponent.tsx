import {observer} from "mobx-react";
import GameStateComponentProps from "../GameStateComponentProps";
import {Component, ReactNode} from "react";
import renderChildGameState from "../../utils/renderChildGameState";
import AeronDamphairDwDAbilityComponent from "./AeronDamphairDwDAbilityComponent";
import QyburnAbilityGameState
    from "../../../common/ingame-game-state/action-game-state/resolve-march-order-game-state/combat-game-state/before-combat-house-card-abilities-game-state/qyburn-ability-game-state/QyburnAbilityGameState";
import QyburnAbilityComponent from "./BronnAbilityComponent";
import BeforeCombatHouseCardAbilitiesGameState from "../../../common/ingame-game-state/action-game-state/resolve-march-order-game-state/combat-game-state/before-combat-house-card-abilities-game-state/BeforeCombatHouseCardAbilitiesGameState";
import AeronDamphairDwDAbilityGameState from "../../../common/ingame-game-state/action-game-state/resolve-march-order-game-state/combat-game-state/before-combat-house-card-abilities-game-state/aeron-damphair-dwd-ability-game-state/AeronDamphairDwDAbilityGameState";
import BronnAbilityGameState from "../../../common/ingame-game-state/action-game-state/resolve-march-order-game-state/combat-game-state/before-combat-house-card-abilities-game-state/bronn-ability-game-state/BronnAbilityGameState";
import BronnAbilityComponent from "./BronnAbilityComponent";

@observer
export default class BeforeCombatHouseCardAbilitiesComponent extends Component<GameStateComponentProps<BeforeCombatHouseCardAbilitiesGameState>> {
    render(): ReactNode {
        return renderChildGameState({...this.props, gameState: this.props.gameState.childGameState}, [
            [AeronDamphairDwDAbilityGameState, AeronDamphairDwDAbilityComponent],
            [QyburnAbilityGameState, QyburnAbilityComponent],
            [BronnAbilityGameState, BronnAbilityComponent]
        ]);
    }
}
