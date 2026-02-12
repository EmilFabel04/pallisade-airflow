"""## Astronaut ETL example DAG

This DAG queries the list of astronauts currently in space from the
Open Notify API and prints each astronaut's name and flying craft.

This repo uses Apache Airflow 2.x (see `docker-compose.yml`), so this DAG
uses Airflow 2 TaskFlow imports (`airflow.decorators`) and `Dataset` for
outlets.
"""

from __future__ import annotations

import requests
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from pendulum import datetime


CURRENT_ASTRONAUTS = Dataset("current_astronauts")


@dag(
    start_date=datetime(2025, 4, 22),
    schedule="@daily",
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    tags=["example"],
)
def example_astronauts():
    @task(outlets=[CURRENT_ASTRONAUTS])
    def get_astronauts(**context) -> list[dict]:
        """Fetch astronauts currently in space.

        Pushes `number_of_people_in_space` to XCom and returns a list of dicts
        to be used by downstream mapped tasks.
        """

        try:
            r = requests.get("http://api.open-notify.org/astros.json", timeout=30)
            r.raise_for_status()
            payload = r.json()
            number_of_people_in_space = payload["number"]
            list_of_people_in_space = payload["people"]
        except Exception:
            # Keep the example DAG resilient even if the API is down.
            number_of_people_in_space = 12
            list_of_people_in_space = [
                {"craft": "ISS", "name": "Oleg Kononenko"},
                {"craft": "ISS", "name": "Nikolai Chub"},
                {"craft": "ISS", "name": "Tracy Caldwell Dyson"},
                {"craft": "ISS", "name": "Matthew Dominick"},
                {"craft": "ISS", "name": "Michael Barratt"},
                {"craft": "ISS", "name": "Jeanette Epps"},
                {"craft": "ISS", "name": "Alexander Grebenkin"},
                {"craft": "ISS", "name": "Butch Wilmore"},
                {"craft": "ISS", "name": "Sunita Williams"},
                {"craft": "Tiangong", "name": "Li Guangsu"},
                {"craft": "Tiangong", "name": "Li Cong"},
                {"craft": "Tiangong", "name": "Ye Guangfu"},
            ]

        context["ti"].xcom_push(
            key="number_of_people_in_space", value=number_of_people_in_space
        )
        return list_of_people_in_space

    @task
    def print_astronaut_craft(greeting: str, person_in_space: dict) -> None:
        craft = person_in_space["craft"]
        name = person_in_space["name"]
        print(f"{name} is currently in space flying on the {craft}! {greeting}")

    print_astronaut_craft.partial(greeting="Hello!").expand(
        person_in_space=get_astronauts()
    )


example_astronauts()
