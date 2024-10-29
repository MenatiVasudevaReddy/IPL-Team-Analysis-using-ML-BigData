#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_TEAMS 20
#define MAX_NAME_LEN 50
#define BUFFER_SIZE 1024

typedef struct {
    char name[MAX_NAME_LEN];
    int wins;
    int matches;
} Team;

void initializeTeams(Team teams[], int size) {
    for (int i = 0; i < size; i++) {
        teams[i].name[0] = '\0';
        teams[i].wins = 0;
        teams[i].matches = 0;
    }
}

int findOrAddTeam(Team teams[], const char* name, int* teamCount) {
    for (int i = 0; i < *teamCount; i++) {
        if (strcmp(teams[i].name, name) == 0) {
            return i;
        }
    }
    if (*teamCount < MAX_TEAMS - 1) {
        strcpy(teams[*teamCount].name, name);
        teams[*teamCount].wins = 0;
        teams[*teamCount].matches = 0;
        (*teamCount)++;
        return *teamCount - 1;
    }
    return -1;
}

void updateMatchesAndWins(Team teams[], char team1[], char team2[], char winningTeam[], int* teamCount) {
    int index1 = findOrAddTeam(teams, team1, teamCount);
    int index2 = findOrAddTeam(teams, team2, teamCount);

    if (index1 != -1) teams[index1].matches++;
    if (index2 != -1 && strcmp(team1, team2) != 0) teams[index2].matches++;

    if (strlen(winningTeam) > 0) { // Ignore "NA" or empty winning teams
        int winIndex = findOrAddTeam(teams, winningTeam, teamCount);
        if (winIndex != -1) teams[winIndex].wins++;
    }
}

int main() {
    FILE* fp = fopen("E:\\VIT_SEM2_Projects\\Bigdata_DA\\IPL_Matches_2008_2022.csv", "r");
    if (!fp) {
        perror("Failed to open file");
        return 1;
    }

    Team teams[MAX_TEAMS];
    int teamCount = 0;
    initializeTeams(teams, MAX_TEAMS);

    char buffer[BUFFER_SIZE];
    fgets(buffer, BUFFER_SIZE, fp); // Skip header

    while (fgets(buffer, BUFFER_SIZE, fp)) {
        char team1[MAX_NAME_LEN], team2[MAX_NAME_LEN], winningTeam[MAX_NAME_LEN];
        // Assuming correct columns for Team1, Team2, and WinningTeam
        sscanf(buffer, "%*[^,],%*[^,],%*[^,],%*[^,],%*[^,],%49[^,],%49[^,],%*[^,],%*[^,],%*[^,],%*[^,],%49[^,]", team1, team2, winningTeam);

        updateMatchesAndWins(teams, team1, team2, winningTeam, &teamCount);
    }
    fclose(fp);

    float maxWinPercentage = 0, minWinPercentage = 100;
    char strongestTeam[MAX_NAME_LEN], weakestTeam[MAX_NAME_LEN];

    printf("Team Wins and Matches Played:\n");
    for (int i = 0; i < teamCount; i++) {
        // Check if the team has won at least one match or has a winning percentage greater than 0
        float winPercentage = teams[i].matches > 0 ? (float)teams[i].wins / teams[i].matches * 100 : 0;
        if (winPercentage > 0) {
            printf("%-30s - Wins: %3d, Matches Played: %3d, Winning Percentage: %.2f%%\n",
                   teams[i].name, teams[i].wins, teams[i].matches, winPercentage);

            // Update the strongest and weakest teams
            if (winPercentage > maxWinPercentage) {
                maxWinPercentage = winPercentage;
                strcpy(strongestTeam, teams[i].name);
            }
            if (winPercentage < minWinPercentage) {
                minWinPercentage = winPercentage;
                strcpy(weakestTeam, teams[i].name);
            }
        }
    }

    printf("\nStrongest Team: %s (%.2f%%)\n", strongestTeam, maxWinPercentage);
    printf("Weakest Team: %s (%.2f%%)\n", weakestTeam, minWinPercentage);

    return 0;
}
