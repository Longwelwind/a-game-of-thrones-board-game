import PlayerMusteringGameState, {
    Mustering,
    PlayerMusteringType
} from "../../common/ingame-game-state/westeros-game-state/mustering-game-state/player-mustering-game-state/PlayerMusteringGameState";
import {observer} from "mobx-react";
import React, {Component, ReactNode} from "react";
import Region from "../../common/ingame-game-state/game-data-structure/Region";
import BetterMap from "../../utils/BetterMap";
import {observable} from "mobx";
import House from "../../common/ingame-game-state/game-data-structure/House";
import * as _ from "lodash";
import {Button} from "react-bootstrap";
import GameStateComponentProps from "./GameStateComponentProps";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Order from "../../common/ingame-game-state/game-data-structure/Order";
import ConsolidatePowerOrderType
    from "../../common/ingame-game-state/game-data-structure/order-types/ConsolidatePowerOrderType";

@observer
export default class PlayerMusteringComponent extends Component<GameStateComponentProps<PlayerMusteringGameState>> {
    @observable selectedRegion: Region | null;
    @observable musterings = new BetterMap<Region, Mustering[]>();

    regionClickListener: any;
    orderClickListener: any;
    highlightRegionListener: any;
    highlightOrderListener: any;

    get house(): House {
        return this.props.gameState.house;
    }

    render(): ReactNode {
        return (
            <Row>
                <Col xs={12}>
                    {this.props.gameState.type == PlayerMusteringType.STARRED_CONSOLIDATE_POWER ? (
                        <>House {this.house.name} can use one of its starred consolidate power to muster units</>
                    ) : this.props.gameState.type == PlayerMusteringType.MUSTERING_WESTEROS_CARD ? (
                        <>Players can muster units in their controlled castles and fortresses</>
                    ) : this.props.gameState.type == PlayerMusteringType.THE_HORDE_DESCENDS_WILDLING_CARD && (
                        <></>
                    )}
                </Col>
                {this.props.gameClient.doesControlHouse(this.house) ? (
                    <>
                        {this.musterings.size > 0 && (
                            <Col xs={12}>
                                {this.musterings.entries.map(([region, musterings]) => (
                                    <div key={region.id}>
                                        From <strong>{region.name}</strong>:
                                        <ul>
                                            {musterings.map(({region, from, to}, i) => (
                                                <li onClick={() => musterings.splice(i, 1)} key={i}>
                                                    {from ? "Upgrading to " : "Recruiting "} a {to.name} in {region.name}
                                                </li>
                                            ))}
                                        </ul>
                                    </div>
                                ))}
                            </Col>
                        )}
                        <Col xs={12}>
                            {this.props.gameState.type == PlayerMusteringType.STARRED_CONSOLIDATE_POWER ? (
                                <>Click on a starred consolidate power to initiate a recruitment from there.</>
                            ) : this.props.gameState.type == PlayerMusteringType.MUSTERING_WESTEROS_CARD ? (
                                <>Click on a region to initiate a recruitement from there.</>
                            ) : this.props.gameState.type == PlayerMusteringType.THE_HORDE_DESCENDS_WILDLING_CARD && (
                                <>...</>
                            )}
                        </Col>
                        {this.selectedRegion && (
                            <>
                                <Col xs={12}>
                                    From {this.selectedRegion.name}, you can ({this.props.gameState.getPointsLeft(this.selectedRegion, this.musterings)} points left):
                                </Col>
                                <Col xs={12}>
                                    <Row>
                                        {this.props.gameState.getValidMusteringRules(this.selectedRegion, this.musterings).map(({region, rules}) => (
                                            rules.map((r, i) => (
                                                <Col xs="auto" key={region.id + "-" + i}>
                                                    <Button onClick={() => this.addMustering(r)}>
                                                        [{r.cost}] {r.from ? "Upgrade to " : "Recruit "} a {r.to.name} in {region.name}
                                                    </Button>
                                                </Col>
                                            ))
                                        ))}
                                    </Row>
                                </Col>
                            </>
                        )}
                        <Col xs={12}>
                            <Row className="justify-content-between">
                                <Col xs="auto">
                                    <Button disabled={!this.canSubmit()} onClick={() => this.submit()}>Submit</Button>
                                </Col>
                                <Col>
                                    {this.props.gameState.type == PlayerMusteringType.STARRED_CONSOLIDATE_POWER &&
                                         this.selectedRegion != null && (
                                            <Button onClick={() => this.submitForPT()}>Get 2 power tokens</Button>
                                        )
                                    }
                                </Col>
                                <Col xs="auto">
                                    {this.musterings.size > 0 && (
                                        <Button variant="danger" onClick={() => this.reset()}>Reset</Button>
                                    )}
                                </Col>
                            </Row>
                        </Col>
                    </>
                ) : (
                    <Col xs={12} className="text-center">
                        Waiting for {this.house.name}...
                    </Col>
                )}
            </Row>
        );
    }

    canSubmit(): boolean {
        if (this.props.gameState.type == PlayerMusteringType.STARRED_CONSOLIDATE_POWER) {
            return this.musterings.size == 1;
        } else if (this.props.gameState.type == PlayerMusteringType.MUSTERING_WESTEROS_CARD) {
            return true;
        } else {
            return false;
        }
    }

    addMustering(rule: Mustering): void {
        if (this.selectedRegion == null) {
            return;
        }

        const musterings: Mustering[] = this.musterings.tryGet(this.selectedRegion, []);

        musterings.push({
            from: rule.from,
            to: rule.to,
            region: rule.region
        });

        this.musterings.set(this.selectedRegion, musterings);

        if (this.props.gameState.getPointsLeft(this.selectedRegion, this.musterings) == 0) {
            this.selectedRegion = null;
        }
    }

    submit(): void {
        this.props.gameState.muster(this.musterings);
    }

    componentDidMount(): void {
        this.props.mapControls.onRegionClick.push(this.regionClickListener = (r: Region) => this.onRegionClick(r));
        this.props.mapControls.onOrderClick.push(this.orderClickListener = (r: Region, o: Order) => this.onOrderClick(r, o));
        this.props.mapControls.shouldHighlightRegion.push(this.highlightRegionListener = (r: Region) => this.shouldHighlightRegion(r));
        this.props.mapControls.shouldHighlightOrder.push(this.highlightOrderListener = (r: Region, o: Order) => this.shouldHighlightOrder(r, o));
    }

    componentWillUnmount(): void {
        _.pull(this.props.mapControls.onRegionClick, this.regionClickListener);
        _.pull(this.props.mapControls.onOrderClick, this.orderClickListener);
        _.pull(this.props.mapControls.shouldHighlightRegion, this.highlightRegionListener);
        _.pull(this.props.mapControls.shouldHighlightOrder, this.highlightOrderListener);
    }

    private shouldHighlightRegion(region: Region): boolean {
        if (this.props.gameClient.doesControlHouse(this.props.gameState.house)) {
            if (this.selectedRegion == null) {
                if (this.props.gameState.type == PlayerMusteringType.MUSTERING_WESTEROS_CARD) {
                    return this.props.gameState.game.world.regions.values
                        .filter(r => r.getController() == this.props.gameState.house && r.castleLevel > 0)
                        .includes(region);
                }
            } else {
                return this.selectedRegion == region;
            }
        }

        return false;
    }

    private onRegionClick(region: Region) {
        if (region.getController() == this.props.gameState.house) {
            if (region.castleLevel == 0) {
                return;
            }

            this.selectedRegion = region;
        }
    }

    private shouldHighlightOrder(r: Region, o: Order): boolean {
        if (this.props.gameState.type == PlayerMusteringType.STARRED_CONSOLIDATE_POWER) {
            if (this.selectedRegion == null) {
                return this.props.gameClient.doesControlHouse(r.getController()) && o.type instanceof ConsolidatePowerOrderType;
            }
        }

        return false;
    }

    private onOrderClick(r: Region, o: Order): void {
        if (this.props.gameState.type == PlayerMusteringType.STARRED_CONSOLIDATE_POWER) {
            if (this.selectedRegion == null) {
                if (this.props.gameClient.doesControlHouse(r.getController()) && o.type instanceof ConsolidatePowerOrderType) {
                    this.selectedRegion = r;
                }
            }
        }
    }

    reset(): void {
        this.selectedRegion = null;
        this.musterings = new BetterMap<Region, Mustering[]>();
    }

    submitForPT(): void {
        if (this.selectedRegion == null) {
            return;
        }

        // Send a mustering with only one region, with no recruitements.
        // This acts as a consolidate power
        this.props.gameState.muster(new BetterMap([
            [this.selectedRegion, []]
        ]));
    }
}
