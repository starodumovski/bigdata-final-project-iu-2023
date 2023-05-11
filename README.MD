# Project description

This project was based on working in **hadoop** using sandbox hdp.
The dataset for the project is **movies** (their release dates, streaming dates, authors, genres, etc.) and **reviews** (dates of reviews, scores, binary types of scores, content)

And we were trying to implement the model to predict the movies' ratings based on critics past assessments, defining possible biases of reviews on genres

# Final project, BigData course in IU-2023.

It contains the following directories:
- `data/` - the dataset files.
- `models/` - the Spark ML models.
- `notebooks/` - Jupyter or Zeppelin notebooks of the project.
- `output/` - the output directory for storing the results of the project.
- `scripts/` -  `.sh` scripts and `.py` scripts of the pipeline.
- `sql/` - `.sql` and `.hql` files.

`requirements.txt` lists the Python packages needed for running your Python scripts.

# How to start the pipeline:

Being in the folder where the `main.sh` is placed, run the following commant in terminal:
```bash
bash main.sh
```
