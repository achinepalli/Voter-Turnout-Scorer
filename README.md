# Voter-Turnout-Scorer
Rule based method to calculate voter propensity from a given Voter File. Voters are given points for each election they participate in, with each election weighted by general turnout (higher weights for lower turnout elections and vice-versa). These points are then normamlized amongst each registration cohort. Improves on existing methods by imputing recent registerants' scores using bayesian inference. 
