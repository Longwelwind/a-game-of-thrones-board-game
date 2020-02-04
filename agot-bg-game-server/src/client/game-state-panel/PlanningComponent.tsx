import {Component} from "react";
import PlanningGameState from "../../common/ingame-game-state/planning-game-state/PlanningGameState";
import orders from "../../common/ingame-game-state/game-data-structure/orders";
import {observable} from "mobx";
import {observer} from "mobx-react";
import Order from "../../common/ingame-game-state/game-data-structure/Order";
import React from "react";
import Region from "../../common/ingame-game-state/game-data-structure/Region";
import * as _ from "lodash";
import ListGroupItem from "react-bootstrap/ListGroupItem";
import GameStateComponentProps from "./GameStateComponentProps";
import Button from "react-bootstrap/Button";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import OrderGridComponent from "./utils/OrderGridComponent";
import ConfirmDialog from "./../utils/ConfirmDialog"

@observer
export default class PlanningComponent extends Component<GameStateComponentProps<PlanningGameState>> {
    @observable selectedOrder: Order | null;
    regionClickListener: any;
    orderClickListener: any;
    highlightRegionListener: any;
    dialog: ConfirmDialog | null;

    render() {
        return (
            <>
                <ListGroupItem>
                    <Row>
                        <Col xs={12}>
                            Assign an order to each region in which one of your unit is present.
                        </Col>
                        {this.props.gameClient.authenticatedPlayer && (
                            <Col xs={12}>
                                <OrderGridComponent orders={orders.values}
                                                    selectedOrder={this.selectedOrder}
                                                    availableOrders={
                                                        this.props.gameState.getAvailableOrders(this.props.gameClient.authenticatedPlayer.house)
                                                    }
                                                    onOrderClick={o => this.selectOrder(o)}/>
                            </Col>
                        )}
                        <Col xs={12}>
                            {this.props.gameClient.authenticatedPlayer && !this.props.gameState.readyPlayers.includes(this.props.gameClient.authenticatedPlayer) ? (
                                <Row className="justify-content-center">
                                    <Col xs="auto">
                                        <Button
                                            disabled={this.props.gameState.isReady(this.props.gameClient.authenticatedPlayer)}
                                            onClick={() => this.onReadyClick()}
                                        >
                                            Ready
                                        </Button>
                                    </Col>
                                </Row>
                            ) : (
                                <div className="text-center">
                                    Waiting for {this.props.gameState.getNotReadyPlayers().map(p => p.house.name).join(', ')}...
                                </div>
                            )}
                        </Col>
                    </Row>
                </ListGroupItem>
                <ConfirmDialog ref={(component) => { this.dialog = component }}></ConfirmDialog>
            </>
        );
    }

    selectOrder(order: Order) {
        if (this.selectedOrder == order) {
            this.selectedOrder = null;
        } else {
            this.selectedOrder = order;
        }
    }

    isOrderAvailable(order: Order): boolean {
        if (!this.props.gameClient.authenticatedPlayer) {
            return false;
        }
        return this.props.gameState.isOrderAvailable(this.props.gameClient.authenticatedPlayer.house, order);
    }

    componentDidMount(): void {
        this.props.mapControls.onRegionClick.push(this.regionClickListener = (r: Region) => this.onRegionClick(r));
        this.props.mapControls.onOrderClick.push(this.orderClickListener = (r: Region, o: Order) => this.onOrderClick(r, o));
        this.props.mapControls.shouldHighlightRegion.push(this.highlightRegionListener = (r: Region) => this.shouldHighlightRegion(r));
    }

    componentWillUnmount(): void {
        _.pull(this.props.mapControls.onRegionClick, this.regionClickListener);
        _.pull(this.props.mapControls.onOrderClick, this.orderClickListener);
        _.pull(this.props.mapControls.shouldHighlightRegion, this.highlightRegionListener);
    }

    onRegionClick(region: Region): void {
        if (!this.selectedOrder) {
            return;
        }

        if (this.props.gameClient.authenticatedPlayer && region.getController() != this.props.gameClient.authenticatedPlayer.house) {
            return;
        }

        this.props.gameState.assignOrder(region, this.selectedOrder);
        this.selectedOrder = null;
    }

    onOrderClick(region: Region, _order: Order) {
        // Clicking on a placed order removes it
        if (this.props.gameClient.authenticatedPlayer && region.getController() != this.props.gameClient.authenticatedPlayer.house) {
            return;
        }

        this.props.gameState.assignOrder(region, this.selectedOrder);
    }

    shouldHighlightRegion(r: Region): boolean {
        if (this.props.gameClient.authenticatedPlayer) {
            if (this.selectedOrder != null) {
                return this.props.gameState.getPossibleRegionsForOrders(this.props.gameClient.authenticatedPlayer.house).includes(r);
            }
        }

        return false;
    }

    onReadyClick(): void {
        const house = this.props.gameClient.authenticatedPlayer ? this.props.gameClient.authenticatedPlayer.house : null;
        if (!house || !this.dialog) {
            this.props.gameState.ready();
            return;
        }

        if (!this.props.gameState.areOrdersAssignedToAllPossibleRegions(house)) {
            this.dialog.show({
                body: (
                    <p>
                        You haven't assigned an Order token to all of your areas.<br />Continue anyway?
                    </p>
                ),
                title: null,
                noAction: null,
                yesAction: () => {
                    this.props.gameState.ready();
                }
            });
        } else {
            this.props.gameState.ready();
        }
    }
}
