import {Component, ReactNode} from "react";
import ResolveSingleMarchOrderGameState
    from "../../common/ingame-game-state/action-game-state/resolve-march-order-game-state/resolve-single-march-order-game-state/ResolveSingleMarchOrderGameState";
import React from "react";
import {observable} from "mobx";
import Region from "../../common/ingame-game-state/game-data-structure/Region";
import Unit from "../../common/ingame-game-state/game-data-structure/Unit";
import * as _ from "lodash";
import {observer} from "mobx-react";
import {Button, Form} from "react-bootstrap";
import BetterMap from "../../utils/BetterMap";
import GameStateComponentProps from "./GameStateComponentProps";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import OverlayTrigger from "react-bootstrap/OverlayTrigger";
import Tooltip from "react-bootstrap/Tooltip";
import {OrderOnMapProperties, RegionOnMapProperties, UnitOnMapProperties} from "../MapControls";
import PartialRecursive from "../../utils/PartialRecursive";
import House from "../../common/ingame-game-state/game-data-structure/House";

@observer
export default class ResolveSingleMarchOrderComponent extends Component<GameStateComponentProps<ResolveSingleMarchOrderGameState>> {
    @observable selectedMarchOrderRegion: Region | null;
    @observable selectedUnits: Unit[] = [];
    @observable plannedMoves = new BetterMap<Region, Unit[]>();
    @observable leavePowerToken: boolean | undefined = undefined;
    canLeavePowerToken = false;
    canLeavePowerTokenReason = "";

    modifyRegionsOnMapCallback: any;
    modifyUnitsOnMapCallback: any;
    modifyOrdersOnMapCallback: any;

    get house(): House {
        return this.props.gameState.house;
    }

    get isVassalHouse(): boolean {
        return this.props.gameState.ingame.isVassalHouse(this.house);
    }

    render(): ReactNode {
        const allUnitsLeft = this.selectedMarchOrderRegion ? this.props.gameState.haveAllUnitsLeft(this.selectedMarchOrderRegion, this.plannedMoves) : false;
        return (
            <>
                <Col xs={12} className="text-center">
                    House <b>{this.house.name}</b> must resolve one of
                    its March Orders.
                </Col>
                {this.props.gameClient.doesControlHouse(this.house) ? (
                    <>
                        <Col xs={12} className="text-center">
                            {this.selectedMarchOrderRegion == null ? (
                                "Click on one of your March Orders."
                            ) : this.selectedUnits.length == 0 && !allUnitsLeft ? (
                                <>Click on a subset of the troops in <b>{this.selectedMarchOrderRegion.name}</b>.</>
                            ) : !allUnitsLeft ? (
                                <>Click on a neighbouring region, or click on other units in <b>{this.selectedMarchOrderRegion.name}</b>.</>
                            ) : (<></>)}
                        </Col>
                        {this.selectedMarchOrderRegion && this.plannedMoves.size > 0 && (
                            <Col xs={12} className="mt-2">
                                <div>
                                    Planned moves from <b>{this.selectedMarchOrderRegion.name}</b>:
                                    <ul>
                                        {this.plannedMoves.entries.map(([region, units]) => (
                                            <li key={`planned-move_${region.id}`}>
                                                {units.map(u => u.type.name).join(", ")} =&gt; {region.name}
                                            </li>
                                        ))}
                                    </ul>
                                </div>
                            </Col>
                        )}
                        {this.selectedMarchOrderRegion != null && (
                            <>
                                {allUnitsLeft && this.renderLeavePowerToken(this.selectedMarchOrderRegion)}
                                <Col xs={12}>
                                    <Row className="justify-content-center">
                                        <Col xs="auto">
                                            <Button
                                                onClick={() => this.confirm()}
                                                disabled={this.leavePowerToken == undefined}
                                            >
                                                Confirm
                                            </Button>
                                        </Col>
                                        <Col xs="auto">
                                            <Button
                                                variant="danger"
                                                onClick={() => this.reset()}
                                            >
                                                Cancel
                                            </Button>
                                        </Col>
                                    </Row>
                                </Col>
                            </>
                        )}
                    </>
                ) : (
                    <Col xs={12} className="text-center">
                        Waiting for {this.house.name}...
                    </Col>
                )}
            </>
        );
    }

    componentWillMount(): any {
        this.componentWillUpdate();
    }

    componentWillUpdate(): any {
        const {success, reason} = this.selectedMarchOrderRegion
            ? this.props.gameState.canLeavePowerToken(this.selectedMarchOrderRegion, this.plannedMoves)
            : {success: false, reason: "no-all-units-go"}

        if (this.canLeavePowerToken != success) {
            // Set undefined here to actively force a user decision when a move was done which changed canLeavePowerToken
            this.leavePowerToken = undefined;
        }

        this.canLeavePowerToken = success;
        this.canLeavePowerTokenReason = reason;

        if (!this.canLeavePowerToken) {
            this.leavePowerToken = false;
        }

        if (this.isVassalHouse && this.canLeavePowerToken) {
            this.leavePowerToken = true;
        }
    }

    renderLeavePowerToken(startingRegion: Region): ReactNode | null {
        return this.plannedMoves.size > 0 && (
            <Col xs={12} className="text-center">
                <OverlayTrigger overlay={
                    <Tooltip id={"leave-power-token"}>
                        {this.canLeavePowerTokenReason == "already-capital" ? (
                            <>Your capital is always controlled by your house, thus not requiring a Power
                                token to be left when leaving the area to keep control of it.</>
                        ) : this.canLeavePowerTokenReason == "already-power-token" ? (
                            <>A Power token is already present.</>
                        ) : this.canLeavePowerTokenReason == "no-power-token-available" ? (
                            "You don't have any available Power token."
                        ) : this.canLeavePowerTokenReason == "not-a-land" ? (
                            "Power tokens can only be left on land areas."
                        ) : this.canLeavePowerTokenReason == "no-all-units-go" ? (
                            "All units must leave the area in order to leave a Power token."
                        ) : this.canLeavePowerTokenReason == "vassals-always-leave-power-token" ? (
                            "Vassals always leave a Power token."
                        ) : "Leaving a Power token in an area maintain the control your house has on it, even"
                            + " if all units your units leave the area."}
                    </Tooltip>
                }>
                    <Form>
                        <fieldset>
                            <Form.Group>
                                <Col xs={12} className="mb-0 pb-0">
                                    <Form.Label>
                                        Do you want to leave a Power token in <b>{startingRegion.name}</b> to keep control?
                                    </Form.Label>
                                </Col>
                                <Col xs={12} className="mt-0 pt-0">
                                    <Form.Check
                                        id="chk-leave-pt"
                                        name="leave-pt-radios"
                                        inline
                                        type="radio"
                                        label="Yes"
                                        checked={this.leavePowerToken}
                                        onChange={() => {this.leavePowerToken = true;}}
                                        disabled={!this.canLeavePowerToken || this.isVassalHouse}/>
                                    <Form.Check
                                        id="chk-dont-leave-pt"
                                        name="leave-pt-radios"
                                        inline
                                        type="radio"
                                        label="No"
                                        checked={this.leavePowerToken == false}
                                        onChange={() => {this.leavePowerToken = false;}}
                                        disabled={!this.canLeavePowerToken || this.isVassalHouse}/>
                                </Col>
                            </Form.Group>
                        </fieldset>
                    </Form>
                </OverlayTrigger>
            </Col>
        );
    }

    onUnitClick(region: Region, unit: Unit): void {
        this.selectedUnits.push(unit);
    }

    isUnitAvailable(unit: Unit): boolean {
        if (this.selectedUnits.indexOf(unit) != -1) {
            return false;
        }

        if (this.plannedMoves.values.some(units => units.indexOf(unit) != -1)) {
            return false;
        }

        return true;
    }

    onRegionClick(region: Region): void {
        const alreadyGoingUnits = this.plannedMoves.has(region) ? this.plannedMoves.get(region) as Unit[] : [];

        const newGoingArmy = alreadyGoingUnits.concat(this.selectedUnits);

        this.plannedMoves.set(region, newGoingArmy);

        // Make non combat moves directly visible by help of new/removedUnits to easily restore the original gameState by resetting new/removedUnits
        if (this.selectedMarchOrderRegion && !this.props.gameState.doesMoveTriggerAttack(region)) {
            region.newUnits = newGoingArmy;
            this.selectedMarchOrderRegion.removedUnits = _.concat(this.selectedMarchOrderRegion.removedUnits, newGoingArmy);
        }

        this.selectedUnits = [];
    }

    onOrderClick(region: Region): void {
        this.selectedMarchOrderRegion = region;
    }

    reset(): void {
        if (this.selectedMarchOrderRegion) {
            this.selectedMarchOrderRegion.removedUnits = [];
        }
        this.plannedMoves.keys.forEach(r => r.newUnits = []);

        this.selectedMarchOrderRegion = null;
        this.selectedUnits = [];
        this.plannedMoves = new BetterMap<Region, Unit[]>();
        this.leavePowerToken = undefined;
    }

    confirm(): void {
        if (this.leavePowerToken == undefined) {
            return;
        }

        if (!this.selectedMarchOrderRegion) {
            return;
        }

        if(this.plannedMoves.size == 0) {
            if(!confirm("Do you want to remove your March Order?")) {
                return;
            }
        }

        this.props.gameState.sendMoves(
            this.selectedMarchOrderRegion,
            this.plannedMoves,
            this.leavePowerToken
        );

        this.reset();
    }

    modifyOrdersOnMap(): [Region, PartialRecursive<OrderOnMapProperties>][] {
        if (this.props.gameClient.doesControlHouse(this.house)) {
            if (this.selectedMarchOrderRegion == null) {
                return this.props.gameState.getRegionsWithMarchOrder().map(r => [
                    r,
                    {highlight: {active: true}, onClick: () => this.onOrderClick(r)}
                ]);
            }
        }

        return [];
    }

    modifyUnitsOnMap(): [Unit, PartialRecursive<UnitOnMapProperties>][] {
        if (this.props.gameClient.doesControlHouse(this.house)) {
            const marchableUnits = this.selectedMarchOrderRegion != null ? this.props.gameState.getValidMarchUnits(this.selectedMarchOrderRegion).filter(u => this.isUnitAvailable(u)) : [];
            const attackingUnits = _.flatMap(this.plannedMoves.entries.filter(([r, _u]) => this.props.gameState.doesMoveTriggerAttack(r)).map(([_r, u]) => u));

            return _.concat(marchableUnits, attackingUnits).map(u => [
                u,
                {
                    highlight: { active: true, color: attackingUnits.includes(u) ? "red" : "white"},
                    onClick: () => marchableUnits.includes(u) && !attackingUnits.includes(u) ? this.onUnitClick(this.selectedMarchOrderRegion as Region, u) : null
                }
            ]);
        }

        return [];
    }

    modifyRegionsOnMap(): [Region, PartialRecursive<RegionOnMapProperties>][] {
        if (this.props.gameClient.doesControlHouse(this.house)) {
            const targetRegions = this.selectedMarchOrderRegion != null && this.selectedUnits.length > 0 ? this.props.gameState.getValidTargetRegions(this.selectedMarchOrderRegion, this.plannedMoves.entries, this.selectedUnits) : [];
            const combatRegions = this.plannedMoves.keys.filter(r => this.props.gameState.doesMoveTriggerAttack(r));

            return _.concat(targetRegions, combatRegions).map(r => [
                r,
                {
                    highlight: {active: true, color: combatRegions.includes(r) ? "yellow" : "white"},
                    onClick: () => targetRegions.includes(r) ? this.onRegionClick(r) : null
                }
            ]);
        }

        return [];
    }

    componentDidMount(): void {
        this.props.mapControls.modifyOrdersOnMap.push(this.modifyOrdersOnMapCallback = () => this.modifyOrdersOnMap());
        this.props.mapControls.modifyUnitsOnMap.push(this.modifyUnitsOnMapCallback = () => this.modifyUnitsOnMap());
        this.props.mapControls.modifyRegionsOnMap.push(this.modifyRegionsOnMapCallback = () => this.modifyRegionsOnMap());

    }

    componentWillUnmount(): void {
        _.pull(this.props.mapControls.modifyOrdersOnMap, this.modifyOrdersOnMapCallback);
        _.pull(this.props.mapControls.modifyUnitsOnMap, this.modifyUnitsOnMapCallback);
        _.pull(this.props.mapControls.modifyRegionsOnMap, this.modifyRegionsOnMapCallback);

    }
}
