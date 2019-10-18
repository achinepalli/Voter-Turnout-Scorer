# Voter-Turnout-Scorer
Rule based method to calculate voter propensity from a given Voter File written in Python and Spark.

Voters are given points for each election they participate in, with each election weighted by general turnout (higher weights for lower turnout elections and vice-versa). These points are then normmalized amongst each registration cohort. Improves on existing methods by imputing recent registerants' scores using bayesian inference. 
