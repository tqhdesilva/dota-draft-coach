# dota-draft-coach

# Data
https://s3-us-west-2.amazonaws.com/dota-data/opendota_matches.csv

https://s3-us-west-2.amazonaws.com/dota-data/valve_cm.csv

https://s3-us-west-2.amazonaws.com/dota-data/valve_hs.csv

# Gathering Data
https://github.com/tqhdesilva/dota_api

##### What's your project
DotA draft coach is a predictor and coaching system for drafts

##### What's the problem?
DotA2 is a very popular test bed for artificial intelligence and analytics. Teams
are beginning to get serious about using the data for their advantage in drafting
and preparing for tournaments, going so far as to hire full-time analysts to inform
their players' decision making. AI research has been on-going and well publicized
in DotA2, for example OpenAI showed off their 1v1 DotA2 AI at this past International.


DotA 2 is a game that I've played a lot but I could never really get into captain's
mode drafting, since it was so intimidating. It was like a whole new game on top of
the actual game. Although there are already several drafting and picking recommenders,
I wanted to see if I could improve on what has been done in this space and train
an updated predictor. Although there is a lot of additional analysis that can be
done on in-game data such as item purchases, gpm, xpm, map movement, etc., I wanted
to restrict my analysis to purely the drafting stage and if possible, captain's
mode in particular.


Ideally we would like to take into account individual player preference and complex
hero interactions into our analysis. At the very least we would like to be able to
predict the winner of the match based on the pick. If possible we would like to
build a recommender for each phase of the pick/ban phase and provide access to the
recommender via RESTful API. We based our modeling and initial assumptions off of
a couple of papers detailing similar projects.

##### How Did You Do It?
We gathered three different datasets from two different APIs. The first data set
we gathered was from OpenDotA. This data set consisted of detailed match information
including the picks and bans, duration, players, mmr estimates of the players, and
hero rating for each player. The data set was unfortunately dating back from 2014
to present.

 While gathering
data via OpenDotA was straightforward, getting anything useful through Valve's API
involved reading out-dated and sparse documentation, building a complex multithreaded
application deployed on AWS, and then running API requests for many days due to low
rate limits and unreliability.


The second data set we gathered was through Valve's own web API. We gathered nearly
50000 CM games occurring over the last month. We weren't able to gather more detailed
data on player profiles.

The third data set we gathered was again through Valve's web API. This time we
queried only games in the highest skill bracket of DotA matchmaking games. Data gathering
is still ongoing and we are currently at about 20000 games over the course of a
few days.

Our third data set is the most similar to what was used in the literature. We were
able to get similar results to the literature so far. The first data set was chosen
to focus on order of picks and bans in anticipation of building models based on each
stage of the draft. The second model is similar to the first but over a shorter time
period. The second data set had noticeably better signal than the first one, but still
fell short of results in the literature.

We stored the first data set locally. The second and third data sets were stored
on RDS and gathered using several EC2 instances with different API keys to avoid
being rate limited.

##### Lessons Learned From the Data
The main lesson we learned about the problem is the importance of model segmentation.
Our initial assumption was that segmentation would be important for game mode. People
would likely pick differently in CM than AP or RD and especially AR. This may be true,
but your CM only data sets showed less signal for unrelated reasons. It's still possible
we can get better results if we predict on CM only. The important factors to segment
on turned out to be when the match was played and skill bracket. The second and third
round of models did much better thanks to predicting on only a certain segment of each.


One big surprise in the modeling stage was how well logistic regression performed
in comparison to non-parametric learners such as decision trees. One of my initial
assumptions was that the draft's influence on outcome would be highly non-linear,
with many interactions between heroes on the same and opposing teams. The high
performance of logistic regression seems to contradict this assumption. However,
in one of the paper's the author did logistic regression with interaction terms
drawn from winrates of co-picking pairs of heroes. The co-picking model showed
significant improvement over the match-up only model.

###### Next Steps
The next step would be to build recommendation system for each stage of the draft.
We would likely be forced to use the second data set, since taking every combination
of the picks is not viable. We need to have ordered picks.


Another area we can investigate is synergistic and antagonistic relationships between
heroes. We can do this either through polynomial feature engineering or by doing
what the aforementioned paper did.

Finally, a Bayesian learner or latent feature selection model could be applicable here.
