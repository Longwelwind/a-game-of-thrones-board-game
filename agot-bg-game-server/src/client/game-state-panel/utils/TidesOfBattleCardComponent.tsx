import {Component, default as React, ReactNode} from "react";
import {observer} from "mobx-react";
import { TidesOfBattleCard } from "../../../common/ingame-game-state/game-data-structure/static-data-structure/tidesOfBattleCards";
import OverlayTrigger from "react-bootstrap/OverlayTrigger";
import tidesOfBattleImages from "../../../client/tidesOfBattleImages";

interface TidesOfBattleCardComponentProps {
    tidesOfBattleCard: TidesOfBattleCard;
}

@observer
export default class TidesOfBattleCardComponent extends Component<TidesOfBattleCardComponentProps> {
    render(): ReactNode {
        return (
            <OverlayTrigger
                overlay={
                    <div className="vertical-game-card small" style={{
                        backgroundImage: `url(${tidesOfBattleImages.get(this.props.tidesOfBattleCard.id)})`
                    }}/>
                }
                popperConfig={{modifiers: {preventOverflow: {boundariesElement: "viewport"}}}}
                delay={{show: 120, hide: 0}}
                placement="auto"
            >
                <div
                    className="vertical-game-card hover-weak-outline tiny"
                    style={{
                        backgroundImage: `url(${tidesOfBattleImages.get(this.props.tidesOfBattleCard.id)})`
                    }}
                />
            </OverlayTrigger>
        );
    }
}
