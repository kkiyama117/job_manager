//! FlowRecipe `g16-opt-parse` — opt --afterok--> parse。

use crate::recipes::job::FlowRecipe;

pub struct G16OptParse;

impl FlowRecipe for G16OptParse {
    fn name(&self) -> &'static str {
        "g16-opt-parse"
    }
    fn summary(&self) -> &'static str {
        "g16 geometry optimization -> afterok -> cclib result.json (self-contained, kudpc)"
    }
    fn nodes(&self) -> &'static [(&'static str, &'static str)] {
        &[("opt", "g16_opt"), ("parse", "parse_g16_out")]
    }
    fn edges(&self) -> &'static [(&'static str, &'static str, &'static str)] {
        &[("opt", "parse", "afterok")]
    }
    fn wiring(&self) -> &'static [(&'static str, &'static str, &'static str, &'static str)] {
        &[("parse", "gaussian_out", "opt", "gaussian_out")]
    }
}
