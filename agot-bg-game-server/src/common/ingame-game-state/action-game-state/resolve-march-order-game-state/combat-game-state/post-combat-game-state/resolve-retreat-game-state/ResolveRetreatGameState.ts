import GameState from "../../../../../../GameState";
import Region from "../../../../../game-data-structure/Region";
import {ServerMessage} from "../../../../../../../messages/ServerMessage";
import {ClientMessage} from "../../../../../../../messages/ClientMessage";
import Player from "../../../../../Player";
import World from "../../../../../game-data-structure/World";
import EntireGame from "../../../../../../EntireGame";
import PostCombatGameState from "../PostCombatGameState";
import SelectUnitsGameState, {SerializedSelectUnitsGameState} from "../../../../../select-units-game-state/SelectUnitsGameState";
import CombatGameState from "../../CombatGameState";
import House from "../../../../../game-data-structure/House";
import SelectRegionGameState, {SerializedSelectRegionGameState} from "../../../../../select-region-game-state/SelectRegionGameState";
import Unit from "../../../../../game-data-structure/Unit";
import Game from "../../../../../game-data-structure/Game";
import IngameGameState from "../../../../../IngameGameState";
import BetterMap from "../../../../../../../utils/BetterMap";
import _ from "lodash";

export default class ResolveRetreatGameState extends GameState<
    PostCombatGameState,
    SelectRegionGameState<ResolveRetreatGameState> | SelectUnitsGameState<ResolveRetreatGameState>
> {
    retreatRegion: Region | null = null;

    get combat(): CombatGameState {
        return this.postCombat.combat;
    }

    get game(): Game {
        return this.postCombat.game;
    }

    get ingame(): IngameGameState {
        return this.parentGameState.parentGameState.ingameGameState;
    }

    get postCombat(): PostCombatGameState {
        return this.parentGameState;
    }

    get world(): World {
        return this.game.world;
    }

    get entireGame(): EntireGame {
        return this.postCombat.entireGame;
    }

    firstStart(): void {
        // Due to Arianne Martell ASoS we have to proceed destroyUnitsThatCannotRetreatOrAreWounded again:
        this.postCombat.destroyUnitsWhichCannotRetreatOrAreWounded();

        if (this.postCombat.loserCombatData.army.length == 0) {
            // If there are no units left, end resolve retreat
            this.postCombat.onResolveRetreatFinish();
            return;
        }

        // House cards may override the chooser of the retreat location
        // and this has to be considered when getting valid retreat regions
        const overridenChooser = this.getOverridenRetreatLocationChooser(this.postCombat.loser);
        const finalChooser = overridenChooser ? overridenChooser : this.postCombat.loser;

        const possibleRetreatRegions = this.getValidRetreatRegions(
            this.combat.attackingRegion,
            this.combat.defendingRegion,
            this.combat.attacker,
            this.postCombat.loser,
            this.combat.attackingArmy,
            this.postCombat.loserCombatData.army,
            finalChooser
        );

        if (possibleRetreatRegions.length == 0) {
            // No retreat regions available
            if (this.combat.attacker == this.postCombat.loser) {
                // If attacker lost all units from attacking region will be destroyed
                this.destroyAllUnits(this.combat.attackingRegion, this.postCombat.loser);
            } else {
                // If defender lost all units from defender are destroyed
                this.destroyAllUnits(this.combat.defendingRegion, this.postCombat.loser);
            }

            this.postCombat.onResolveRetreatFinish();
            return;
        } else {
            this.setChildGameState(new SelectRegionGameState(this))
                    .firstStart(finalChooser, possibleRetreatRegions);
        }
    }

    private destroyAllUnits(region: Region, affectedHouse: House): void {
        const unitsToKill = region.units.values;
        unitsToKill.forEach(u => region.units.delete(u.id));

        // Remove the units off the army
        const houseCombatData = this.combat.houseCombatDatas.get(affectedHouse);
        houseCombatData.army = _.difference(houseCombatData.army, unitsToKill);

        this.entireGame.broadcastToClients({
            type: "combat-change-army",
            region: region.id,
            house: affectedHouse.id,
            army: houseCombatData.army.map(u => u.id)
        });

        this.ingame.broadcastRemoveUnits(region, unitsToKill)

        this.ingame.log({
            type: "retreat-failed",
            house: affectedHouse.id,
            isAttacker: affectedHouse == this.combat.attacker,
            region: region.id
        });

        this.ingame.log({
            type: "retreat-casualties-suffered",
            house: affectedHouse.id,
            units: unitsToKill.map(u => u.type.id)
        }, true);
    }

    onSelectRegionFinish(_house: House, retreatRegion: Region, resolvedAutomatically: boolean): void {
        const loserCombatData = this.postCombat.loserCombatData;
        const army = loserCombatData.army;
        this.retreatRegion = retreatRegion;

        this.ingame.log({
            type: "retreat-region-chosen",
            house: this.postCombat.loser.id,
            regionFrom: this.combat.defendingRegion.id,
            regionTo: retreatRegion.id
        }, resolvedAutomatically);

        // Check if this retreat region require casualties
        const casualtiesCount = this.getCasualtiesOfRetreatRegion(retreatRegion);
        if (casualtiesCount > 0) {
            this.setChildGameState(new SelectUnitsGameState(this)).firstStart(this.postCombat.loser, army, casualtiesCount);
            return;
        }

        // Otherwise, proceed with no casualties
        this.onSelectUnitsEnd(this.postCombat.loser, [], false);
    }

    onSelectUnitsEnd(_loser: House, selectedUnits: [Region, Unit[]][], resolvedAutomatically: boolean): void {
        if (!this.retreatRegion) {
            throw new Error("retreatRegion must be set at this point!");
        }

        // Kill the casualties of the retreat selected by the loser
        selectedUnits.forEach(([region, units]) => {
            units.forEach(u => region.units.delete(u.id));
            this.postCombat.loserCombatData.army = _.difference(this.postCombat.loserCombatData.army, units);

            this.entireGame.broadcastToClients({
                type: "combat-change-army",
                house: this.postCombat.loser.id,
                region: this.postCombat.loserCombatData.region.id,
                army: this.postCombat.loserCombatData.army.map(u => u.id)
            });

            this.ingame.broadcastRemoveUnits(region, units);

            this.ingame.log({
                type: "retreat-casualties-suffered",
                house: this.postCombat.loser.id,
                units: units.map(u => u.type.id)
            }, resolvedAutomatically);
        });

        const armyLeft = this.postCombat.loserCombatData.army;

        if (armyLeft.length > 0) {
            if (!this.combat.areWoundsPrevented(this.postCombat.loser)) {
                // Mark those as wounded
                armyLeft.forEach(u => u.wounded = true);

                this.ingame.sendMessageToPlayersWhoCanSeeRegion({
                    type: "units-wounded",
                    regionId: this.postCombat.loserCombatData.region.id,
                    unitIds: armyLeft.map(u => u.id)
                }, this.postCombat.loserCombatData.region);
            }

            // Retreat those unit to this location
            armyLeft.forEach(u => {
                this.postCombat.loserCombatData.region.units.delete(u.id);
                this.retreatRegion?.units.set(u.id, u);
                u.region = this.retreatRegion as Region;
            });

            const from = this.postCombat.loserCombatData.region;

            if (!this.ingame.fogOfWar) {
                this.entireGame.broadcastToClients({
                    type: "move-units",
                    from: from.id,
                    to: this.retreatRegion.id,
                    units: armyLeft.map(u => u.id),
                    isRetreat: true
                });
            } else {
                this.ingame.players.values.forEach(p => {
                    const visibleRegions = this.ingame.getVisibleRegionsForPlayer(p);
                    if (visibleRegions.includes(from) && visibleRegions.includes(this.retreatRegion as Region)) {
                        this.entireGame.sendMessageToClients([p.user], {
                            type: "move-units",
                            isRetreat: true,
                            from: from.id,
                            to: (this.retreatRegion as Region).id,
                            units: armyLeft.map(u => u.id)
                        });
                    } else {
                        this.ingame.broadcastRemoveUnits(from, armyLeft, false)
                        this.ingame.broadcastAddUnits(this.retreatRegion as Region, armyLeft);
                    }
                });
            }
        }

        this.postCombat.onResolveRetreatFinish();
    }

    getOverridenRetreatLocationChooser(retreater: House): House | null {
        return this.combat.getOrderResolutionHouseCard().reduce((s, h) => {
            const houseCard = this.combat.houseCombatDatas.get(h).houseCard;

            if (houseCard == null) {
                return s;
            }

            return houseCard.ability && !s ? houseCard.ability.overrideRetreatLocationChooser(this.postCombat, h, houseCard, retreater) : s;
        }, null);
    }

    onPlayerMessage(player: Player, message: ClientMessage): void {
        this.childGameState.onPlayerMessage(player, message);
    }

    onServerMessage(message: ServerMessage): void {
        this.childGameState.onServerMessage(message);
    }

    getValidRetreatRegions(attackingRegion: Region, defendingRegion: Region, attacker: House, loser: House, attackingArmy: Unit[], loserArmy: Unit[], finalChooser: House): Region[] {
        const possibleRetreatRegions = this.world.getValidRetreatRegions(
            defendingRegion,
            loser,
            loserArmy
        );

        if(attacker == loser) {
            // Attacker lost the battle. Check if he can retreat back to the attacking region.
            // Because it might be blocked for retreat if ...
            const attackingRegionIsBlockedForRetreat =
            // ... attacking region is a capital ...
            attackingRegion.superControlPowerToken != null
            // ... but not the attackers capital and ...
            && attackingRegion.superControlPowerToken != attacker
            // ... attacker left no Power Token and ...
            && attackingRegion.controlPowerToken != attacker
            // ... all units marched to combat
            && attackingArmy.length == attackingRegion.units.size;

            if(finalChooser == attacker) {
                // Final chooser is the attacker himself
                // We can omit the supply check as attacker will never exceed supply by retreating back to the attacking region
                return attackingRegionIsBlockedForRetreat
                    // If attacking region is blocked there is no possible retreat area.
                    ? []
                    // Otherwise there is only the attacking region
                    : [attackingRegion];
            } else {
                // Final chooser is someone else.
                // He can decide to retreat the units to a valid region adjacent to the combat
                if (attackingRegionIsBlockedForRetreat) {
                    // But if attacking region is blocked it must be filtered out
                    _.pull(possibleRetreatRegions, attackingRegion);
                } else if (!possibleRetreatRegions.includes(attackingRegion)) {
                    // Dragons may have flown into the embattled area,
                    // so the starting region may not be one of the possible retreat regions.
                    possibleRetreatRegions.push(attackingRegion);
                }
            }
        } else {
            // Filter out the attacking region as due to Berric Dondarrion (and maybe some other effects)
            // the attacking region may not be occupied by the enemy anymore and therefore isn't filtered by
            // getValidRetreatRegions anymore
            _.pull(possibleRetreatRegions, attackingRegion);
        }

        if (possibleRetreatRegions.length == 0) {
            return [];
        }

        // No need for else path here.
        // If defender lost the battle he or the final chooser
        // has to choose a retreat region from possible retreat regions

        // Now take supply into account and calculate the casualty values per possible retreat region
        const casualtiesPerRegion =
            possibleRetreatRegions.map(r => [r, this.getCasualtiesOfRetreatRegion(r)] as [Region, number]);

        // Get lowest casualty value
        const lowestCasualty = _.min(casualtiesPerRegion.map(([_, c]) => c));

        if (lowestCasualty == loserArmy.length) {
            // If lowest casualty equals to the unit count of the whole army there is no valid retreat region
            return [];
        }

        // Filter regions for lowest casualty value
        return casualtiesPerRegion.filter(([_, c]) => c == lowestCasualty).map(([r, _]) => r);
    }

    getCasualtiesOfRetreatRegion(retreatRegion: Region): number {
        // If retreatRegion is attackingRegion the attacker lost the battle
        // and retreats back from where he came from. In that case we don't need
        // to calculate casualties as retreating back to attackingRegion will always be
        // supply compliant at that point (if it is blocked for retreat
        // has been checked earlier).
        // Furthermore we have to do this extra processing for the time being
        // because the attacking units are still present in attackingRegion
        // and therefore hasTooMuchArmies with addedUnits overload will double
        // the army size for the attackingRegion which could result
        // in an invalid supply violation.
        if (retreatRegion == this.postCombat.combat.attackingRegion) {
            return 0;
        }

        // To find the number of casualties that a specific retreat region will inflict,
        // simulate a movement of an increasing number of units and find at which number of units
        // the user has too much armies for their supplies.
        // This number represents the number of units the player will be able to keep.
        // Assumption: all units have the same weight when it comes to supply.
        const retreatingArmy = this.postCombat.loserCombatData.army;

        const indexTooMuch = retreatingArmy.findIndex((_, i) => {
            const retreatingUnits = retreatingArmy.slice(0, i + 1);

            return this.game.hasTooMuchArmies(
                this.postCombat.loser,
                new BetterMap([[retreatRegion, retreatingUnits.map(u => u.type)]]),
                new BetterMap([[this.postCombat.loserCombatData.region, retreatingArmy]])
            );
        });

        if (indexTooMuch > -1) {
            return retreatingArmy.length - indexTooMuch;
        } else {
            return 0;
        }
    }

    serializeToClient(admin: boolean, player: Player | null): SerializedResolveRetreatGameState {
        return {
            type: "resolve-retreat",
            retreatRegion: this.retreatRegion ? this.retreatRegion.id : null,
            childGameState: this.childGameState.serializeToClient(admin, player)
        };
    }

    static deserializeFromServer(postCombat: PostCombatGameState, data: SerializedResolveRetreatGameState): ResolveRetreatGameState {
        const resolveRetreat = new ResolveRetreatGameState(postCombat);

        resolveRetreat.retreatRegion = data.retreatRegion ? postCombat.game.world.regions.get(data.retreatRegion) : null;
        resolveRetreat.childGameState = resolveRetreat.deserializeChildGameState(data.childGameState);

        return resolveRetreat;
    }

    deserializeChildGameState(data: SerializedResolveRetreatGameState["childGameState"]): ResolveRetreatGameState["childGameState"] {
        switch (data.type) {
            case "select-region":
                return SelectRegionGameState.deserializeFromServer(this, data);
            case "select-units":
                return SelectUnitsGameState.deserializeFromServer(this, data);
        }
    }
}

export interface SerializedResolveRetreatGameState {
    type: "resolve-retreat";
    retreatRegion: string | null;
    childGameState: SerializedSelectRegionGameState | SerializedSelectUnitsGameState;
}
