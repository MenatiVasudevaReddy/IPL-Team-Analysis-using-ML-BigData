#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_TEAMS 30
#define MAX_NAME_LEN 100
#define BUFFER_SIZE 1024

typedef struct {
    char name[MAX_NAME_LEN];
    int wins;
    int matches;
    double winning_percentage;
    int cluster;
} Team;

Team teams[MAX_TEAMS];
int teamCount = 0;

void initializeTeams() {
    #pragma omp parallel for
    for (int i = 0; i < MAX_TEAMS; i++) {
        teams[i].name[0] = '\0';
        teams[i].wins = 0;
        teams[i].matches = 0;
        teams[i].winning_percentage = 0.0;
        teams[i].cluster = -1;
    }
}

int findOrAddTeam(const char* name) {
    int index = -1;
    #pragma omp parallel for
    for (int i = 0; i < teamCount; i++) {
        if (strcmp(teams[i].name, name) == 0) {
            index = i;
        }
    }
    if (index != -1) return index;

    #pragma omp critical
    {
        if (teamCount < MAX_TEAMS - 1) {
            strcpy(teams[teamCount].name, name);
            teamCount++;
            index = teamCount - 1;
        }
    }
    return index;
}

void updateMatchesAndWins(char team1[], char team2[], char winningTeam[]) {
    int index1 = findOrAddTeam(team1);
    int index2 = findOrAddTeam(team2);

    if (index1 != -1) teams[index1].matches++;
    if (index2 != -1) teams[index2].matches++;

    if (strlen(winningTeam) >= 0 && strcmp(winningTeam, "NA") != 0) {
        int winIndex = findOrAddTeam(winningTeam);
        if (winIndex != -1) teams[winIndex].wins++;
    }
}

void calculateWinningPercentagesAndAssignClusters() {
    #pragma omp parallel for
    for (int i = 0; i < teamCount; i++) {
        if (teams[i].matches > 0) {
            teams[i].winning_percentage = (double)teams[i].wins / teams[i].matches * 100;
            if (teams[i].winning_percentage >= 50) teams[i].cluster = 1; // Top Performer
            else if (teams[i].winning_percentage >= 30) teams[i].cluster = 2; // Midfield Performers
            else teams[i].cluster = 3; // Lower Performers
        }
    }
}

void printClusters() {
    printf(" Cluster 1 (Top Performer) - Master Thread ID: %d\n", omp_get_thread_num());
    #pragma omp parallel for
    for (int i = 0; i < teamCount; i++) {
        if (teams[i].cluster == 1 && teams[i].winning_percentage > 0) {
            printf("  - %s: %.2f%% (Won %d out of %d matches) - Thread ID: %d\n",
                   teams[i].name, teams[i].winning_percentage, teams[i].wins, teams[i].matches, omp_get_thread_num());
        }
    }

    printf("\n Cluster 2 (Midfield Performers) - Master Thread ID: %d\n", omp_get_thread_num());
    #pragma omp parallel for
    for (int i = 0; i < teamCount; i++) {
        if (teams[i].cluster == 2 && teams[i].winning_percentage > 0) {
            printf("  - %s: %.2f%% (Won %d out of %d matches) - Thread ID: %d\n",
                   teams[i].name, teams[i].winning_percentage, teams[i].wins, teams[i].matches, omp_get_thread_num());
        }
    }

    printf("\n Cluster 3 (Lower Performers) - Master Thread ID: %d\n", omp_get_thread_num());
    #pragma omp parallel for
    for (int i = 0; i < teamCount; i++) {
        if (teams[i].cluster == 3 && teams[i].winning_percentage > 0) {
            printf("  - %s: %.2f%% (Won %d out of %d matches) - Thread ID: %d\n",
                   teams[i].name, teams[i].winning_percentage, teams[i].wins, teams[i].matches, omp_get_thread_num());
        }
    }
}

int main() {
    initializeTeams();
    FILE* fp = fopen("E:\\VIT_SEM2_Projects\\Bigdata_DA\\IPL_Matches_2008_2022.csv", "r");
    if (!fp) {
        perror("Failed to open file");
        return 1;
    }

    char buffer[BUFFER_SIZE];
    fgets(buffer, BUFFER_SIZE, fp); // Skip header

    while (fgets(buffer, BUFFER_SIZE, fp)) {
        char team1[MAX_NAME_LEN], team2[MAX_NAME_LEN], winningTeam[MAX_NAME_LEN];
        sscanf(buffer, "%*[^,],%*[^,],%*[^,],%*[^,],%*[^,],%99[^,],%99[^,],%*[^,],%*[^,],%*[^,],%*[^,],%99[^,]", team1, team2, winningTeam);

        updateMatchesAndWins(team1, team2, winningTeam);
    }
    fclose(fp);

    double start_time = omp_get_wtime(); // Start timing

    calculateWinningPercentagesAndAssignClusters();
    printClusters();

    double end_time = omp_get_wtime(); // End timing
    printf("Time taken: %f seconds\n", end_time - start_time);

    return 0;
}
