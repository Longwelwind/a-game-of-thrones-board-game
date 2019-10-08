import SilenceAtTheWall from "./SilenceAtTheWall";
import BetterMap from "../../../../utils/BetterMap";
import PreemptiveRaidWildlingCardType from "./PreemptiveRaidWildlingCardType";
import CrowKillers from "./CrowKillers";


export const silenceAtTheWall = new SilenceAtTheWall(
    "silence-at-the-wall", "Silence at the Wall",
    "Nothing happens",
    "Nothing happens",
    "Nothing happens"
);
export const preemptiveRaid = new PreemptiveRaidWildlingCardType(
    "preemptive-raid", "Preemptive Raid",
    "Chooses one of the following",
    "Nothing happens",
    ""
);
export const crowKillers = new CrowKillers(
    "crow-killers", "Crow Killers",
    "Replaces all of his Knights with available Footmen. Any knight unable to be replaced is destroyed.",
    "Replaces 2 of their Knights with available Footmen. Any knight unable to be replaced is destroyed.",
    "May immediately replace up to 2 of his Footmen, anywhere, with available Knights."
);

const wildlingCardTypes = new BetterMap([
    [silenceAtTheWall.id, silenceAtTheWall],
    [preemptiveRaid.id, preemptiveRaid],
    [crowKillers.id, crowKillers]
]);

export default wildlingCardTypes;
