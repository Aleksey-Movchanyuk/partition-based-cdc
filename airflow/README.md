### Prerequisites
Python 3.8.5


### Virtual Environment
#### Linux
```
sudo apt update
sudo apt upgrade
sudo apt install python3 python3-pip python3-venv

sudo apt-get install -y --no-install-recommends \
        freetds-bin \
        krb5-user \
        ldap-utils \
        libffi6 \
        libsasl2-2 \
        libsasl2-modules \
        libssl1.1 \
        locales  \
        lsb-release \
        sasl2-bin \
        sqlite3 \
        unixodbc

python3 -m venv venv
source venv/bin/activate 
```

#### MacOS
```
python3 -m venv venv
source venv/bin/activate 
```

#### Windows
```
pip install virtualenv
virtualenv wvenv
wvenv\Scripts\activate
```

#### WLS
https://keisugiyasu.medium.com/how-to-avoid-a-few-pitfalls-of-wsl2-with-ubuntu-distribution-83150ff5cc9


### Set Environemt Variablers
#### Linux
```
export AIRFLOW_HOME="$PWD"
export AIRFLOW_VERSION=2.3.4
export PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
export CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
```

#### MacOS
```
export AIRFLOW_HOME="$PWD"
export AIRFLOW_VERSION=2.3.4
export PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
export CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
```

#### Windows
```
$env:AIRFLOW_HOME="C:\Users\aleksey\Documents\Practiq Trader\repos\practiq-trader\airflow"
$env:AIRFLOW_VERSION=2.3.4
$env:PYTHON_VERSION=3.8
$env:CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-2.1.0/constraints-3.8.txt"

echo $env:AIRFLOW_HOME
echo $env:AIRFLOW_VERSION
echo $env:PYTHON_VERSION
echo $env:CONSTRAINT_URL
```

### Install Dependencies
```
pip install --upgrade pip==20.2.4

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
pip install apache-airflow-providers-oracle
```

### Useful commands
```
export AIRFLOW_HOME="$PWD"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD"/dags

airflow db init
airflow db reset

airflow users create -r Admin -u admin -e admin@datatogo.uk -f admin -l user -p admin

airflow connections import connections/dev.json
airflow variables import variables/dev.json
```