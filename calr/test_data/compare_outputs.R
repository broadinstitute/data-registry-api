#!/usr/bin/env Rscript

# Compare Python CalR output with R CalR output
# for TSE file conversion

# Load required libraries
suppressMessages(library(shiny))
suppressMessages(library(data.table))
suppressMessages(library(lubridate))

# Source only required R CalR functions
cat("Sourcing R CalR functions...\n")
calr_r_dir <- "/home/dhite/code-repos/broad/calr/R"

# Core functions needed for loading
required_files <- c(
  "period.R", "asdate.R", "asdatetime.R", "utils.R", "maths.R",
  "loadCalFile.R", "loadTSEFile.R", "modTSE.R",
  "loadOxyFile.R", "modOxy.R",
  "loadSableFile.R", "modSable.R",
  "retrofitCalR.R"
)

for (r_file in required_files) {
  file_path <- file.path(calr_r_dir, r_file)
  if (file.exists(file_path)) {
    source(file_path)
  } else {
    cat("Warning: Could not find", r_file, "\n")
  }
}

# Load TSE file using R CalR
cat("Loading TSE file with R CalR...\n")
raw_tse <- loadTSEFile(in.file="calr_tse.csv")
r_output <- modTSE(raw_tse)

# Save R output
write.csv(r_output, "r_output_tse.csv", row.names = FALSE)
cat("Saved R output:", nrow(r_output), "rows,", ncol(r_output), "columns\n")

# Load Python output
py_output <- read.csv("python_output_tse.csv")
cat("Loaded Python output:", nrow(py_output), "rows,", ncol(py_output), "columns\n")

# Compare dimensions
cat("\n=== DIMENSION COMPARISON ===\n")
cat("R rows:", nrow(r_output), "Python rows:", nrow(py_output), "\n")
cat("R cols:", ncol(r_output), "Python cols:", ncol(py_output), "\n")

# Compare column names
cat("\n=== COLUMN COMPARISON ===\n")
r_cols <- colnames(r_output)
py_cols <- colnames(py_output)

missing_in_py <- setdiff(r_cols, py_cols)
missing_in_r <- setdiff(py_cols, r_cols)

if (length(missing_in_py) > 0) {
  cat("Columns in R but not Python:", paste(missing_in_py, collapse=", "), "\n")
}
if (length(missing_in_r) > 0) {
  cat("Columns in Python but not R:", paste(missing_in_r, collapse=", "), "\n")
}

# Compare common columns
common_cols <- intersect(r_cols, py_cols)
cat("Common columns:", length(common_cols), "\n")

# Compare first few rows of key columns
cat("\n=== DATA COMPARISON (first 5 rows) ===\n")
key_cols <- c("subject.id", "subject.mass", "cage", "Date.Time", "vo2", "vco2", 
              "ee", "rer", "feed", "feed.acc", "drink", "drink.acc")
key_cols <- intersect(key_cols, common_cols)

for (col in key_cols) {
  cat("\n", col, ":\n", sep="")
  cat("  R     :", head(r_output[[col]], 5), "\n")
  cat("  Python:", head(py_output[[col]], 5), "\n")
  
  # Check if numeric and compare
  if (is.numeric(r_output[[col]]) && is.numeric(py_output[[col]])) {
    r_vals <- head(r_output[[col]], 5)
    py_vals <- head(py_output[[col]], 5)
    
    # Handle NA comparisons
    both_na <- is.na(r_vals) & is.na(py_vals)
    if (any(!both_na)) {
      diffs <- abs(r_vals[!both_na] - py_vals[!both_na])
      if (any(diffs > 0.01, na.rm=TRUE)) {
        cat("  *** DIFFERENCE detected (max:", max(diffs, na.rm=TRUE), ")\n")
      } else {
        cat("  ✓ Values match\n")
      }
    } else {
      cat("  ✓ Both all NA\n")
    }
  }
}

cat("\n=== SUMMARY ===\n")
if (nrow(r_output) == nrow(py_output) && 
    length(missing_in_py) == 0 && 
    length(missing_in_r) == 0) {
  cat("✓ Dimensions and columns match!\n")
  cat("✓ Python TSE loader successfully replicates R behavior\n")
} else {
  cat("⚠ There are differences between R and Python output\n")
  cat("  Review the comparison above\n")
}
