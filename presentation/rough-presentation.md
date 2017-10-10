# Background
- 5v5
- complex set of features
- 114 heroes
- useful problem set: cheaters, leavers, drafting, betting, live analytics, AI research

# Motivation
- drafting is hard to practice
- feedback is unclear
- teams now employ analysts to inform drafting
- rule changes are now allowing coaches to be involved in drafting
- previous literature only uses a handful of models
- previous literature may need updating for current patch
- just how big of an impact does the draft have on the outcome?

# Data Acquisition
- OpenDotA : CM games from 2013 to present with ordered picks and bans
- D2 api: CM games over the last month
- D2 api: high skill bracket 5v5 games over the past few days
- limit to matches where duration at least 10 minutes

# Modeling
- logistic regression
- random forest
- boosting
