#!/bin/zsh

# Store the individual times in an array
times=()

# Function to perform the curl command
perform_curl() {
    # Insert your curl command in place of <your-curl-command>
    curl -o /dev/null -s -w "%{time_total}\n" -X POST https://api.kpndataregistry.org:8000/api/upload-hermes -H "Dataset: Timing DS" -H "FileName: random_data_100M.csv" -H "Content-Type: multipart/form-data" \
    -F "file=@/home/dhite/code-repos/broad/data-registry-api/scripts/random_data_100M.csv" \
        -H "Metadata: {\"dataSetName\":\"Cypress dataset\",\"cohort\":\"UKBiobank\",\"contactPerson\":\"Point of Contact\",\"dataCollectionStart\":\"2006-01-01T05:00:00.000Z\",\"sex\":\"Male only\",\"dataCollectionEnd\":\"2006-03-01T05:00:00.000Z\",\"ancestry\":\"EU\",\"caseAscertainment\":\"Electronic Health Records\",\"phenotype\":\"T2D\",\"caseDefinition\":\"My case definition\",\"totalSampleSize\":\"989\",\"maleProportionCohort\":\"1\",\"callingAlgorithm\":\"You called, algorithm?\",\"genotypingArray\":\"Array against you\",\"referenceGenome\":\"Hg19\",\"imputationSoftware\":\"Imputate this software\",\"imputationReference\":\"Imputation reference\",\"numberOfVariantsForImputation\":\"47\",\"imputationQualityMeasure\":\"highest quality\",\"relatedIndividualsRemoved\":\"Yes\",\"variantCallRate\":\".22\",\"sampleCallRate\":\".2247\",\"hwePValue\":\".09121\",\"maf\":\"1\",\"otherFilters\":\"My Filters\",\"column_map\":{\"chromosome\":\"CHR\",\"position\":\"BP\",\"alt\":\"OA\",\"reference\":\"EA\",\"eaf\":\"EAF\",\"beta\":\"BETA\",\"stdErr\":\"SE\",\"pValue\":\"P\",\"N case/ events\":\"N\",\"oddsRatio\":\"ODDS_RATIO\"}}" \
        -H "Authorization: bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX25hbWUiOiJkaGl0ZUBicm9hZGluc3RpdHV0ZS5vcmciLCJmaXJzdF9uYW1lIjpudWxsLCJsYXN0X25hbWUiOm51bGwsImVtYWlsIjpudWxsLCJhdmF0YXIiOm51bGwsImlzX2FjdGl2ZSI6dHJ1ZSwicm9sZXMiOlsiYWRtaW4iXSwiZ3JvdXBzIjpbXSwicGVybWlzc2lvbnMiOltdLCJpc19pbnRlcm5hbCI6ZmFsc2UsImFwaV90b2tlbiI6bnVsbCwiaWQiOjQsImV4cCI6MTcyOTExNzUwNn0.kcswMgGUOL7hcySX2S8MOik2DrLXwaOXXUVAswCTMWo"
}

# Loop to perform the curl command 10 times
for i in {1..5}; do
    # Call the perform_curl function and store the result in the array
    times+=($(perform_curl))
done

# Calculate the sum of the times
sum=0.0
for time in $times; do
    sum=$(echo "$sum + $time" | bc)
done

# Calculate the mean time
mean=$(echo "$sum / 10" | bc -l)

# Print the mean time
echo "Mean time spent: $mean seconds"
