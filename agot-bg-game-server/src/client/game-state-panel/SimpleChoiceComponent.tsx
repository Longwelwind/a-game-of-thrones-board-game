import {Component} from "react";
import * as React from "react";
import {observer} from "mobx-react";
import SimpleChoiceGameState from "../../common/ingame-game-state/simple-choice-game-state/SimpleChoiceGameState";
import GameStateComponentProps from "./GameStateComponentProps";
import Button from "react-bootstrap/Button";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";

@observer
export default class SimpleChoiceComponent extends Component<GameStateComponentProps<SimpleChoiceGameState>> {
    render() {
        return (
            <Row>
                <Col xs={12}>
                    {this.props.gameState.description}
                </Col>
                <Col xs={12} className="text-center">
                    {this.props.gameClient.doesControlHouse(this.props.gameState.house) ? (
                        <Row className="justify-content-center">
                            {this.props.gameState.choices.map((s, i) => (
                                <Col xs="auto">
                                    <Button onClick={() => this.choose(i)}>{s}</Button>
                                </Col>
                            ))}
                        </Row>
                    ) : (
                        <>Waiting for {this.props.gameState.house.name}...</>
                    )}
                </Col>
            </Row>
        );
    }

    choose(choice: number) {
        this.props.gameState.choose(choice);
    }
}
