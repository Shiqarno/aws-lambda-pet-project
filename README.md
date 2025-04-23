# AWS Lambda Pet Project

This project demonstrates a simple AWS Lambda function written in Python that
interacts with AWS S3. It's designed as a foundational example for understanding
how to set up and deploy serverless applications using AWS services.

## Features

- **AWS Lambda Function**: Python-based Lambda function that performs
  AWS-triggered operations.
- **AWS S3 Integration**: Reads and writes to Amazon S3.
- **AWS CLI Usage**: Uses AWS CLI for deployment and configuration.
- **Pre-commit Hooks**: Git hooks for enforcing code quality and formatting.
- **Conda Environment**: Easily reproducible environment using `env.yml`.

## Prerequisites

- [Python 3.8 or higher](https://www.python.org/downloads/)
- [Anaconda or Miniconda](https://docs.conda.io/projects/conda/en/latest/user-guide/install/)
- [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html)
- [AWS Account](https://aws.amazon.com/) with Lambda and S3 permissions
- [Pre-commit](https://pre-commit.com/)

## Setup Instructions

1. **Clone the Repository**:

   ```bash
   git clone https://github.com/Shiqarno/aws-lambda-pet-project.git
   cd aws-lambda-pet-project
   ```

2. **Create and Activate Conda Environment**:

   Use the provided `env.yml` to create a reproducible environment.

   ```bash
   conda env create -f env.yml
   conda activate aws-lambda-env  # Replace with the name in your `env.yml`
   ```

3. **Configure AWS CLI**:

   Set up AWS credentials:

   ```bash
   aws configure
   ```

4. **Set Up Pre-commit Hooks**:

   ```bash
   pip install pre-commit
   pre-commit install
   ```

5. **Deploy the Lambda Function**:

   Just run the script (update bucket name, function name, and role ARN
   accordingly):

   ```bash
   python main.py
   ```

## Project Structure

```
├── .flake8                  # Flake8 configuration
├── .gitignore               # Git ignore rules
├── .pre-commit-config.yaml  # Pre-commit hook config
├── LICENSE                  # MIT License
├── README.md                # Project documentation
├── env.yml                  # Conda environment definition
├── lambda_function.py       # Lambda function code
├── settings.yaml            # Environment settings
└── main.py                  # Local entry point
```

## License

This project is licensed under the [MIT License](LICENSE).

## Acknowledgements

Built for learning and experimenting with AWS Lambda and S3 integration using
Python.
