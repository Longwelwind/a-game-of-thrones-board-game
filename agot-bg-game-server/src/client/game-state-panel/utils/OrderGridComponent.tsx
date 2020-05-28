import React, {Component} from "react";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import OverlayTrigger from "react-bootstrap/OverlayTrigger";
import Tooltip from "react-bootstrap/Tooltip";
import orderImages from "../../orderImages";
import classNames = require("classnames");
import Order from "../../../common/ingame-game-state/game-data-structure/Order";

interface OrderGridProps {
    orders: Order[];
    selectedOrder: Order | null;
    availableOrders: Order[];
    onOrderClick: (order: Order) => void;
}

export default class OrderGridComponent extends Component<OrderGridProps> {
    render(): JSX.Element {
        return (
            <Row className="justify-content-center">
                <Col xs="auto">
                    <Row style={{width: "180px"}} className="no-gutters justify-content-center">
                        {this.props.orders.map(o => (
                            <Col xs="auto" className="p-1" key={o.id}>
                                <OverlayTrigger overlay={
                                        <Tooltip id="order-icon-tooltip">
                                            {o.type.tooltipText()}
                                        </Tooltip>
                                    }
                                    placement="bottom"
                                >
                                    <div className={classNames(
                                            "order-icon",
                                            {"clickable": this.isOrderAvailable(o) && this.props.selectedOrder != o},
                                            {"hover-weak-outline": this.isOrderAvailable(o) && this.props.selectedOrder != o},
                                            {"strong-outline": this.props.selectedOrder == o})
                                        }
                                         style={{
                                             backgroundImage: `url(${orderImages.get(o.type.id)})`,
                                             opacity: this.props.availableOrders.includes(o) ? 1 : 0.1
                                         }}
                                         onClick={() => this.onOrderClick(o)}
                                         key={o.id}/>
                                </OverlayTrigger>
                            </Col>
                        ))}
                    </Row>
                </Col>
            </Row>
        );
    }

    isOrderAvailable(o: Order): boolean {
        return this.props.availableOrders.includes(o);
    }

    onOrderClick(o: Order): void {
        if (!this.isOrderAvailable(o)) {
            return;
        }

        this.props.onOrderClick(o);
    }
}
