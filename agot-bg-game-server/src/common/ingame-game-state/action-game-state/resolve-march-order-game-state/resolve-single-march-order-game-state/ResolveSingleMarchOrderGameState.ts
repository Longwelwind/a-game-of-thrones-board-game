import ResolveMarchOrderGameState from "../ResolveMarchOrderGameState";
import GameState from "../../../../GameState";
import House from "../../../game-data-structure/House";
import World from "../../../game-data-structure/World";
import Region from "../../../game-data-structure/Region";
import Unit from "../../../game-data-structure/Unit";
import EntireGame from "../../../../EntireGame";
import {ClientMessage} from "../../../../../messages/ClientMessage";
import Player from "../../../Player";
import ActionGameState from "../../ActionGameState";
import _ from "lodash";
import {ServerMessage} from "../../../../../messages/ServerMessage";
import {observable} from "mobx";
import Game from "../../../game-data-structure/Game";
import BetterMap from "../../../../../utils/BetterMap";
import RegionKind from "../../../game-data-structure/RegionKind";

export default class ResolveSingleMarchOrderGameState extends GameState<ResolveMarchOrderGameState> {
    @observable house: House;

    constructor(resolveMarchOrderGameState: ResolveMarchOrderGameState) {
        super(resolveMarchOrderGameState);
    }

    get entireGame(): EntireGame {
        return this.resolveMarchOrderGameState.entireGame;
    }

    get actionGameState(): ActionGameState {
        return this.resolveMarchOrderGameState.parentGameState;
    }

    get resolveMarchOrderGameState(): ResolveMarchOrderGameState {
        return this.parentGameState;
    }

    get game(): Game {
        return this.resolveMarchOrderGameState.game;
    }

    get world(): World {
        return this.resolveMarchOrderGameState.world;
    }

    /**
     * Server
     */

    firstStart(house: House): void {
        this.house = house;
    }

    onPlayerMessage(player: Player, message: ClientMessage): void {
        if (message.type == "resolve-march-order") {
            if (player.house != this.house) {
                console.warn("Not correct house");
                return;
            }

            const startingRegion = this.world.regions.get(message.startingRegionId);

            const moves = message.moves.map(([regionId, unitIds]) => [
                this.world.regions.get(regionId),
                unitIds.map(uid => startingRegion.units.get(uid))
            ] as [Region, Unit[]]);

            // Check that there is indeed a march order there
            if (!this.getRegionsWithMarchOrder().includes(startingRegion)) {
                console.warn("No march order on startingRegion");
                return;
            }

            if (!this.areValidMoves(startingRegion, moves)) {
                return;
            }

            // Check that at most one move triggers a fight
            const movesThatTriggerAttack = moves.filter(([region, _army]) => this.doesMoveTriggerAttack(region));
            // This has been checked earlier in "this.areValidMoves" but it's never bad
            // to check twice
            if (movesThatTriggerAttack.length > 1) {
                console.warn("More than one move that triggers a fight");
                return;
            }

            const movesThatDontTriggerAttack = _.difference(moves, movesThatTriggerAttack);

            // Check if the player was capable of placing a power token
            if (this.canDecideToLeavePowerToken(startingRegion, new BetterMap(moves)) && message.placePowerToken) {
                startingRegion.controlPowerToken = this.house;
                this.house.powerTokens -= 1;

                this.entireGame.broadcastToClients({
                    type: "change-power-token",
                    houseId: this.house.id,
                    powerTokenCount: this.house.powerTokens
                });

                this.entireGame.broadcastToClients({
                    type: "change-control-power-token",
                    regionId: startingRegion.id,
                    houseId: this.house.id
                });
            }

            // Execute the moves that don't trigger a fight
            movesThatDontTriggerAttack.forEach(([region, units]) => {
                this.resolveMarchOrderGameState.moveUnits(startingRegion, units, region);
            });

            if (movesThatDontTriggerAttack.length > 0) {
                this.entireGame.log(
                    `**${this.house.name}** marched:`,
                    ``,
                    ...movesThatDontTriggerAttack.map(([r, us]) =>
                        `${us.map(u => u.type.name).join(', ')} to **${r.name}**`
                    )
                );
            }

            // If there was a move that trigger a fight, do special processing
            if (movesThatTriggerAttack.length > 0) {
                // There should be only one attack move
                const [region, army] = movesThatTriggerAttack[0];

                // 2 kind of attack moves possible:
                const enemy = region.getController();
                if (enemy) {
                    // Attack against an other house

                    this.entireGame.log(
                        `**${this.house.name}** attacked **${enemy.name}** from **${startingRegion.name}**`,
                        ` to **${region.name}** with ${army.map(u => u.type.name).join(", ")}`
                    );

                    this.resolveMarchOrderGameState.proceedToCombat(startingRegion, region, this.house, enemy, army);
                    return;
                } else {
                    // Attack against a neutral force
                    // That the player put up enough strength against the neutral force was
                    // already checked earlier. No need to re-check it now, just process the attack.
                    region.garrison = 0;
                    this.resolveMarchOrderGameState.moveUnits(startingRegion, army, region);

                    this.entireGame.log(
                        `**${this.house.name}** attacked a neutral force from **${startingRegion.name}**`,
                        ` to **${region.name}** with ${army.map(u => u.type.name).join(", ")}`
                    );

                    this.entireGame.broadcastToClients({
                        type: "change-garrison",
                        region: region.id,
                        newGarrison: region.garrison
                    });
                }
            }

            // Remove the order
            this.actionGameState.ordersOnBoard.delete(startingRegion);
            this.entireGame.broadcastToClients({
                type: "action-phase-change-order",
                region: startingRegion.id,
                order: null
            });

            this.resolveMarchOrderGameState.onResolveSingleMarchOrderGameStateFinish(this.house);
        }
    }

    onServerMessage(_message: ServerMessage): void {
    }

    areValidMoves(startingRegion: Region, moves: [Region, Unit[]][]): boolean {
        return moves.every(
            ([regionToward, army], i) => this.getValidTargetRegions(startingRegion, moves.slice(0, i), army).includes(regionToward)
        );
    }

    /**
     * Gives the list of regions that `movingArmy` can move to, given a starting region
     * and a list of already valid `moves`.
     * @param startingRegion
     * @param moves
     * @param movingArmy
     */
    getValidTargetRegions(startingRegion: Region, moves: [Region, Unit[]][], movingArmy: Unit[]): Region[] {
        const movesThatTriggerAttack = this.getMovesThatTriggerAttack(moves);
        const attackMoveAlreadyPresent = movesThatTriggerAttack.length > 0;

        return this.world.getReachableRegions(startingRegion, this.house, movingArmy)
            // Filter out destinations that are already used
            .filter(r => !moves.map(([r, _a]) => r).includes(r))
            // Check that this new move doesn't trigger another attack
            .filter(r => !attackMoveAlreadyPresent || this.doesMoveTriggerAttack(r))
            // Check that the moves doesn't exceed supply
            .filter(r => !this.doesMoveExceedSupply(startingRegion, new BetterMap(moves.concat([[r, movingArmy]]))))
            // If the move is an attack on a neutral force, then there must be sufficient combat strength
            // to overcome the neutral force
            .filter(r => {
                if (r.getController() == null && r.garrison > 0) {
                    return this.hasEnoughToAttackNeutralForce(movingArmy, r);
                }

                return true;
            });
    }

    getMovesThatTriggerAttack(moves: [Region, Unit[]][]): [Region, Unit[]][] {
        // Moves that trigger an attack are those that go into ennemy territory
        // or a neutral force.
        return moves.filter(([region, _army]) => this.doesMoveTriggerAttack(region));
    }

    doesMoveTriggerAttack(regionToward: Region): boolean {
        const controller = regionToward.getController();
        if (controller != null) {
            if (controller != this.house) {
                // A move that goes into an enemy-controlled territory with no units,
                // but with a garrison is considered an attack.
                return regionToward.units.size > 0 || regionToward.garrison > 0;
            }
        } else {
            return regionToward.garrison > 0;
        }

        return false;
    }

    getRegionsWithMarchOrder(): Region[] {
        return this.actionGameState.getRegionsWithMarchOrderOfHouse(this.house);
    }

    hasEnoughToAttackNeutralForce(army: Unit[], targetRegion: Region): boolean {
        return this.game.getCombatStrengthOfArmy(army, targetRegion.hasStructure) + this.actionGameState.getSupportCombatStrength(this.house, targetRegion) >= targetRegion.garrison;
    }

    sendMoves(startingRegion: Region, moves: BetterMap<Region, Unit[]>, placePowerToken: boolean): void {
        this.entireGame.sendMessageToServer({
            type: "resolve-march-order",
            moves: moves.entries.map(([region, units]) => [region.id, units.map(u => u.id)]),
            startingRegionId: startingRegion.id,
            placePowerToken: placePowerToken
        });
    }

    doesMoveExceedSupply(startingRegion: Region, moves: BetterMap<Region, Unit[]>): boolean {
        return this.game.hasTooMuchArmies(
            this.house,
            new BetterMap(moves.entries.map(([region, units]) => [region, units.map(u => u.type)])),
            new BetterMap([
                [startingRegion, ([] as Unit[]).concat(...moves.values)]
            ])
        );
    }

    serializeToClient(_admin: boolean, _player: Player | null): SerializedResolveSingleMarchOrderGameState {
        return {
            type: "resolve-single-march",
            houseId: this.house.id
        };
    }

    getPhaseName(): string {
        return "Resolve a March Order";
    }

    canDecideToLeavePowerToken(startingRegion: Region, moves: BetterMap<Region, Unit[]>): boolean {
        if (startingRegion.superControlPowerToken == this.house) {
            return false;
        }

        if (startingRegion.controlPowerToken) {
            return false;
        }

        if (this.house.powerTokens == 0) {
            return false;
        }

        if (startingRegion.type.kind != RegionKind.LAND) {
            return false;
        }

        // The player can place a power token if all units go out
        return _.sum(moves.values.map(us => us.length)) == startingRegion.units.size;
    }

    static deserializeFromServer(resolveMarchOrderGameState: ResolveMarchOrderGameState, data: SerializedResolveSingleMarchOrderGameState): ResolveSingleMarchOrderGameState {
        const resolveSingleMarchOrderGameState = new ResolveSingleMarchOrderGameState(resolveMarchOrderGameState);

        resolveSingleMarchOrderGameState.house = resolveMarchOrderGameState.game.houses.get(data.houseId);

        return resolveSingleMarchOrderGameState;
    }
}

export interface SerializedResolveSingleMarchOrderGameState {
    type: "resolve-single-march";
    houseId: string;
}
