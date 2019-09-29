import {Component} from "react";
import GameClient from "./GameClient";
import * as React from "react";
import {observer} from "mobx-react";
import IngameGameState from "../common/ingame-game-state/IngameGameState";
import MapComponent from "./MapComponent";
import MapControls from "./MapControls";
import {HouseCardState} from "../common/ingame-game-state/game-data-structure/house-card/HouseCard";
import ListGroup from "react-bootstrap/ListGroup";
import ListGroupItem from "react-bootstrap/ListGroupItem";
import Card from "react-bootstrap/Card";
import Col from "react-bootstrap/Col";
import Row from "react-bootstrap/Row";
import renderChildGameState from "./utils/renderChildGameState";
import WesterosGameState from "../common/ingame-game-state/westeros-game-state/WesterosGameState";
import WesterosGameStateComponent from "./game-state-panel/WesterosGameStateComponent";
import PlanningGameState from "../common/ingame-game-state/planning-game-state/PlanningGameState";
import PlanningComponent from "./game-state-panel/PlanningComponent";
import ActionGameState from "../common/ingame-game-state/action-game-state/ActionGameState";
import ActionComponent from "./game-state-panel/ActionComponent";
import houseInfluenceImages from "./houseInfluenceImages";
import houseCardsBackImages from "./houseCardsBackImages";
import houseCardImages from "./houseCardImages";
import * as _ from "lodash";
import OverlayTrigger from "react-bootstrap/OverlayTrigger";
import {FontAwesomeIcon} from "@fortawesome/react-fontawesome";
import {faStar} from "@fortawesome/free-solid-svg-icons/faStar";
import Tooltip from "react-bootstrap/Tooltip";
import classNames = require("classnames");
import Badge from "react-bootstrap/Badge";
import barrelImage from "../../public/images/icons/barrel.svg";
import castleImage from "../../public/images/icons/castle.svg";
import stoneThroneImage from "../../public/images/icons/stone-throne.svg";
import ravenImage from "../../public/images/icons/raven.svg";
import diamondHiltImage from "../../public/images/icons/diamond-hilt.svg";
import hourglassImage from "../../public/images/icons/hourglass.svg";
import mammothImage from "../../public/images/icons/mammoth.svg";
import House from "../common/ingame-game-state/game-data-structure/House";
import housePowerTokensImages from "./housePowerTokensImages";
import marked from "marked";

interface IngameComponentProps {
    gameClient: GameClient;
    gameState: IngameGameState;
}

@observer
export default class IngameComponent extends Component<IngameComponentProps> {
    mapControls: MapControls = new MapControls();

    get game() {
        return this.props.gameState.game;
    }

    render() {
        return (
            <>
                <Col xs={12} lg={3}>
                    <Row className="stackable">
                        <Col>
                            <Card>
                                <ListGroup variant="flush">
                                    <ListGroupItem>
                                        <Row className="justify-content-between" style={{fontSize: "24px"}}>
                                            <Col xs="auto">
                                                <OverlayTrigger overlay={
                                                        <Tooltip id="turn">
                                                            <b>Turn</b>
                                                        </Tooltip>
                                                    }
                                                    placement="bottom">
                                                    <div>
                                                        <img src={hourglassImage} style={{marginRight: "5px"}} width={32}/>
                                                        {this.game.turn}
                                                    </div>
                                                </OverlayTrigger>
                                            </Col>
                                            <Col xs="auto">
                                                <OverlayTrigger overlay={
                                                        <Tooltip id="wildling-strength">
                                                            <b>Wildling Strength</b>
                                                        </Tooltip>
                                                    }
                                                    placement="bottom"
                                                >
                                                    <div>
                                                        {this.game.wildlingStrength}
                                                        <img src={mammothImage} width={32} style={{marginLeft: "5px"}}/>
                                                    </div>
                                                </OverlayTrigger>
                                            </Col>
                                        </Row>
                                    </ListGroupItem>
                                    {this.tracks.map(({tracker, stars}, i) => (
                                        <ListGroupItem key={i}>
                                            <Row className="align-items-center">
                                                <Col xs="auto" className="text-center" style={{width: "46px"}}>
                                                    <OverlayTrigger
                                                        overlay={
                                                            <Tooltip id={`tooltip-tracker-${i}`}>
                                                                {i == 0 ? (
                                                                    <>
                                                                        <b>Iron Throne Track</b><br />
                                                                        All ties (except military ones) are decided by the holder
                                                                        of the Iron Throne.<br />
                                                                        Turn order is decided by this tracker.
                                                                    </>
                                                                ) : i == 1 ? (
                                                                    <>
                                                                        <b>Fiefdoms Track</b><br />
                                                                        Once per turn, the holder of Valyrian Sword can use his sword
                                                                        to increase by one the combat strength of his army in a combat.<br />
                                                                        In case of a tie in a combat, the winner is the house which is
                                                                        the highest in this tracker.
                                                                    </>
                                                                ) : (
                                                                    <>
                                                                        <b>Kings's Court Track</b><br />
                                                                        At the end of the Planning Phase, the holder of the Raven may choose
                                                                        to either change one of his placed order, or to see the top card of the
                                                                        wildling deck and decide whether to leave it at the top or to
                                                                        place it at the bottom of the deck.
                                                                    </>
                                                                )}
                                                            </Tooltip>
                                                        }
                                                        placement="right"
                                                    >
                                                        <img src={i == 0 ? stoneThroneImage : i == 1 ? diamondHiltImage : ravenImage} width={32}/>
                                                    </OverlayTrigger>
                                                </Col>
                                                {tracker.map((h, i) => (
                                                    <Col xs="auto" key={h.id}>
                                                        <div className="influence-icon hover-weak-outline"
                                                             style={{backgroundImage: `url(${houseInfluenceImages.get(h.id)})`}}>
                                                        </div>
                                                        <div className="tracker-star-container">
                                                            {stars && i < this.game.starredOrderRestrictions.length && (
                                                                _.range(0, this.game.starredOrderRestrictions[i]).map(i => (
                                                                    <div key={i}>
                                                                        <FontAwesomeIcon
                                                                            style={{color: "#ffc107", fontSize: "9px"}}
                                                                            icon={faStar}/>
                                                                    </div>
                                                                ))
                                                            )}
                                                        </div>
                                                    </Col>
                                                ))}
                                            </Row>
                                        </ListGroupItem>
                                    ))}
                                    <ListGroupItem>
                                        <Row>
                                            <Col xs="auto">
                                                <img width="32px" src={barrelImage} alt="Supply"/>
                                            </Col>
                                            <Col>
                                                <Row className="justify-content-center">
                                                    {this.game.supplyRestrictions.map((allowedArmies, i) => (
                                                        <Col xs="auto" key={i} className="d-flex flex-column align-items-center">
                                                            <div>
                                                            <Badge variant="secondary" style={{fontSize: "14px"}}>
                                                                {i}
                                                            </Badge>
                                                            </div>
                                                            <div className="d-flex">
                                                                <div style={{width: "18px", marginRight: "10px", marginTop: "10px"}}>
                                                                    {this.getHousesAtSupplyLevel(i).map(h => (
                                                                        <OverlayTrigger
                                                                            key={h.id}
                                                                            overlay={
                                                                                <Tooltip id={`supply-house-${h.id}`}>
                                                                                    {h.name}
                                                                                </Tooltip>
                                                                            }
                                                                            placement="right">
                                                                            <div
                                                                                key={h.id}
                                                                                className="supply-icon hover-weak-outline"
                                                                                style={{
                                                                                    backgroundImage: `url(${houseInfluenceImages.get(h.id)})`,
                                                                                    marginTop: "-5px"
                                                                                }}
                                                                            >

                                                                            </div>
                                                                        </OverlayTrigger>
                                                                    ))}
                                                                </div>
                                                                <div>
                                                                    {allowedArmies.map((size, i) => (
                                                                        <div style={{marginBottom: "-6px"}} key={i}>
                                                                            {size}
                                                                        </div>
                                                                    ))}
                                                                </div>
                                                            </div>
                                                        </Col>
                                                    ))}
                                                </Row>
                                            </Col>
                                        </Row>
                                    </ListGroupItem>
                                </ListGroup>
                            </Card>
                        </Col>
                    </Row>
                    <Row>
                        <Col>
                            <Card>
                                <ListGroup variant="flush">
                                    {this.props.gameState.players.values.map(p => (
                                        <ListGroupItem key={p.user.id}>
                                            <Row className="align-items-center">
                                                <Col xs="auto" className="pr-0">
                                                    <FontAwesomeIcon
                                                        className={classNames({"invisible": !this.props.gameClient.isAuthenticatedUser(p.user)})}
                                                        style={{color: p.house.color}}
                                                        icon={faStar}
                                                    />
                                                </Col>
                                                <Col>
                                                    <b style={{"color": p.house.color}}>{p.house.name}</b>
                                                    {" "}
                                                    <a href={`/user/${p.user.id}`} target="_blank">
                                                        <small>{p.user.name}</small>
                                                    </a>
                                                </Col>
                                                <Col xs="auto" className="d-flex align-items-center">
                                                    <div style={{fontSize: "18px"}}>{this.game.getTotalHeldStructures(p.house)}</div>
                                                    <img
                                                        src={castleImage} width={32} className="hover-weak-outline"
                                                        style={{marginLeft: "10px"}}
                                                    />
                                                </Col>
                                                <Col xs="auto" className="d-flex align-items-center">
                                                    <div style={{fontSize: "18px"}}>{p.house.powerTokens}</div>
                                                    <div
                                                        className="house-power-token hover-weak-outline"
                                                        style={{
                                                            backgroundImage: `url(${housePowerTokensImages.get(p.house.id)})`,
                                                            marginLeft: "10px"
                                                        }}
                                                    />
                                                </Col>
                                            </Row>
                                            <Row className="justify-content-center">
                                                {_.sortBy(p.house.houseCards.values, hc => hc.combatStrength).map(hc => (
                                                    <OverlayTrigger
                                                        overlay={
                                                            <div className="house-card-full" style={{backgroundImage: `url(${houseCardImages.get(hc.id)})`}}/>
                                                        }
                                                        delay={{show: 120, hide: 0}}
                                                        placement="right"
                                                        key={hc.id}
                                                    >
                                                        <Col xs="auto">
                                                            <div className="house-card-icon hover-weak-outline"
                                                                 style={{backgroundImage: `url(${hc.state == HouseCardState.AVAILABLE ? houseCardImages.get(hc.id) : houseCardsBackImages.get(p.house.id)})`}}>

                                                            </div>
                                                        </Col>
                                                    </OverlayTrigger>
                                                ))}
                                            </Row>
                                        </ListGroupItem>
                                    ))}
                                </ListGroup>
                            </Card>
                        </Col>
                    </Row>
                </Col>
                <Col xs="auto">
                    <div>
                        <MapComponent
                            gameClient={this.props.gameClient}
                            ingameGameState={this.props.gameState}
                            mapControls={this.mapControls}
                        />
                    </div>
                </Col>
                <Col xs={12} lg={3}>
                    <Row className="stackable">
                        <Col>
                            <Card>
                                <ListGroup variant="flush">
                                    {renderChildGameState({mapControls: this.mapControls, ...this.props}, [
                                        [WesterosGameState, WesterosGameStateComponent],
                                        [PlanningGameState, PlanningComponent],
                                        [ActionGameState, ActionComponent]
                                    ])}
                                </ListGroup>
                            </Card>
                        </Col>
                    </Row>
                    <Row>
                        <Col>
                            <Card style={{height: "350px", overflowY: "scroll"}}>
                                <Card.Body>
                                    {this.props.gameState.parentGameState.gameLogManager.logs.reverse().map((l, i) => (
                                        <Row key={i}>
                                            <Col xs="auto" className="text-muted">
                                                <small>
                                                    {l.time.getHours().toString().padStart(2, "0")}
                                                    :{l.time.getMinutes().toString().padStart(2, "0")}
                                                </small>
                                            </Col>
                                            <Col>
                                                <div className="game-log-content"
                                                    dangerouslySetInnerHTML={{__html: this.compileGameLog(l.message)}}>
                                                </div>
                                            </Col>
                                        </Row>
                                    ))}
                                </Card.Body>
                            </Card>
                        </Col>
                    </Row>
                </Col>
            </>
        );
    }

    get tracks(): {name: string; tracker: House[]; stars: boolean}[] {
        return [
            {name: "Iron Throne", tracker: this.game.ironThroneTrack, stars: false},
            {name: "Fiefdoms", tracker: this.game.fiefdomsTrack, stars: false},
            {name: "King's Court", tracker: this.game.kingsCourtTrack, stars: true},
        ]
    }

    getHousesAtSupplyLevel(supplyLevel: number): House[] {
        return this.game.houses.values.filter(h => h.supplyLevel == supplyLevel);
    }

    compileGameLog(gameLog: string): string {
        return marked(gameLog);
    }
}
