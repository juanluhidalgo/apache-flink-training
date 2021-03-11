package com.apache.flink.training.join;

import com.apache.flink.training.model.CoachTeam;
import com.apache.flink.training.model.GoalTeam;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class JoinFootballTeamCoach implements JoinFunction<GoalTeam, CoachTeam, Tuple2<GoalTeam, CoachTeam>> {

    @Override
    public Tuple2<GoalTeam, CoachTeam> join(GoalTeam footballTeam, CoachTeam coachTeam) throws Exception {
        return Tuple2.of(footballTeam, coachTeam);
    }
}
