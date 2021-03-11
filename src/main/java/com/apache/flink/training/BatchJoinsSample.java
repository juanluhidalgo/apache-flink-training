package com.apache.flink.training;

import com.apache.flink.training.mapper.FromStringToCoachTeam;
import com.apache.flink.training.model.CoachTeam;
import com.apache.flink.training.model.GoalTeam;
import com.apache.flink.training.reduce.GoalCounter;
import com.apache.flink.training.sink.JsonOutputFormat;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.PrintingOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

public class BatchJoinsSample {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //DataSource<FootballTeam> teamsDS = env.fromCollection(getGoalsTeams()).name("Collection of goals");
        //DataSource<CoachTeam> coachDS = env.fromCollection(getCoachTeams()).name("Collection of coachs");

        //DataSource<CoachTeam> coachDS = getCoachTeamsFromCSV(env);
        DataSource<GoalTeam> goalDS = getGoalTeamsFromCSV(env);

        DataSet<CoachTeam> coachDS = getCoachTeamsFromJSON(env);

        DataSet<GoalTeam> goalTeams = goalDS.groupBy("teamName")
                .reduce(new GoalCounter()).name("Count goals per team");

        DataSet<Tuple2<GoalTeam, CoachTeam>> goalTeamsAndCoach = goalTeams.join(coachDS).where("teamName").equalTo("teamName");

        goalTeams.output(new PrintingOutputFormat<>()).name("Writing");
        goalTeams.output(new JsonOutputFormat());
        goalTeamsAndCoach.output(new PrintingOutputFormat<>()).name("Writing");

        env.execute("Batch App");
    }

    private static Collection<CoachTeam> getCoachTeams() {
        return Arrays.asList(CoachTeam.builder().teamName("Real Madrid").coach("Zinedine Zidane").build(),
                             CoachTeam.builder().teamName("Barcelona").coach("Ronald Koeman").build(),
                             CoachTeam.builder().teamName("Atletico Madrid").coach("Diego Pablo Simeone").build());
    }

    private static Collection<GoalTeam> getGoalsTeams() {
        return Arrays.asList(GoalTeam.builder().teamName("Real Madrid").goals(10).build(),
                             GoalTeam.builder().teamName("Barcelona").goals(2).build(),
                             GoalTeam.builder().teamName("Atletico Madrid").goals(2).build(),
                             GoalTeam.builder().teamName("Real Madrid").goals(5).build(),
                             GoalTeam.builder().teamName("Real Madrid").goals(1).build(),
                             GoalTeam.builder().teamName("Atletico Madrid").goals(1).build(),
                             GoalTeam.builder().teamName("Barcelona").goals(3).build());
    }

    private static DataSource<CoachTeam> getCoachTeamsFromCSV(ExecutionEnvironment env) throws URISyntaxException {
        return env.readCsvFile(Paths.get(BatchJoinsSample.class.getResource("/coach_teams.csv").toURI()).toFile().getAbsolutePath())
                .pojoType(CoachTeam.class,
                          "teamName",
                          "coach");
    }

    private static DataSource<GoalTeam> getGoalTeamsFromCSV(ExecutionEnvironment env) throws URISyntaxException {
        return env.readCsvFile(Paths.get(BatchJoinsSample.class.getResource("/goals_teams.csv").toURI()).toFile().getAbsolutePath())
                .pojoType(GoalTeam.class,
                          "teamName",
                          "goals");
    }

    private static DataSet<CoachTeam> getCoachTeamsFromJSON(ExecutionEnvironment env) throws URISyntaxException {
        return env.readTextFile(Paths.get(BatchJoinsSample.class.getResource("/coach_teams.json").toURI()).toFile().getAbsolutePath())
                .map(new FromStringToCoachTeam());
    }


}
