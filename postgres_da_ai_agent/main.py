import os
import dotenv
import argparse
import autogen
from postgres_da_ai_agent.modules import llm
from postgres_da_ai_agent.modules.db import SQLManager
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker

dotenv.load_dotenv()

# Check environment variables
assert os.environ.get("DATABASE_URL"), "DATABASE_URL not found in .env file"
assert os.environ.get("OPENAI_API_KEY"), "OPENAI_API_KEY not found in .inv file"
assert os.environ.get("OPENAI_API_KEY"), "OPENAI_API_KEY not found in .inv file"
assert os.environ.get("OPENAI_BASE_URL"), "OPENAI_BASE_URL not found in .inv file"

# Get environment variables
DB_URL = os.environ.get("DATABASE_URL")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
OPENAI_BASE_URL = os.environ.get("OPENAI_BASE_URL")

# SQLAlchemy setup
engine = create_engine(DB_URL)
Session = sessionmaker(bind=engine)

# Constants
POSTGRES_TABLE_DEFINITIONS_CAP_REF = "TABLE_DEFINITIONS"
RESPONSE_FORMAT_CAP_REF = "RESPONSE_FORMAT"
SQL_DELIMITER = "---------"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--prompt", help="The prompt for the AI")
    args = parser.parse_args()

    if not args.prompt:
        print("Please provide a prompt")
        return

    prompt = f"Fulfill this database query: {args.prompt}. "

    with SQLManager() as db:
        db.connect_with_url(DB_URL)

        table_definitions = db.get_table_definitions_for_prompt()
        #print(table_definitions)

        prompt = llm.add_cap_ref(
            prompt,
            f"Use these {POSTGRES_TABLE_DEFINITIONS_CAP_REF} to satisfy the database query.",
            POSTGRES_TABLE_DEFINITIONS_CAP_REF,
            table_definitions,
        )

        autogen_config_list = [
            {
                'api_type': 'azure',
                'model': 'autogen',
                'api_key': OPENAI_API_KEY,
                'api_base': OPENAI_BASE_URL,
                "api_version": "2023-07-01-preview",
        }
        ]

        #print(autogen_config_list)

        # build the gpt_configuration object
        azureai_config = {
            "use_cache": False,
            "temperature": 0,
            #"config_list": autogen.config_list_from_models(["gpt-4"]),
            #"config_list": autogen.config_list_from_models(["gpt-4-1106-preview"]),
            #"config_list": autogen.config_list_from_models(["gpt-3.5-turbo"]),
            "config_list": autogen_config_list,
            "request_timeout": 120,
            "functions": [
                {
                    "name": "run_sql",
                    "description": "Run a SQL query against the postgres database",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "sql": {
                                "type": "string",
                                "description": "The SQL query to run",
                            }
                        },
                        "required": ["sql"],
                    },
                }
            ],
        }

        # build the function map
        function_map = {
            "run_sql": db.run_sql,
        }

        # create our terminate msg function
        def is_termination_msg(content):
            have_content = content.get("content", None) is not None
            if have_content and "APPROVED" in content["content"]:
                return True
            return False

        COMPLETION_PROMPT = "If everything looks good, respond with APPROVED"
        COMPANY_INFORMATION = "You work at a multinational company that deals with security and this is a database of alerts from cameras. They are using AI on the edge, and use software to handle some of the alerts. They use 'talk downs' or 'audio warnings' to speak to potential intruders."

        USER_PROXY_PROMPT = (
            "A human admin. Interact with the Product Manager to discuss the plan. Plan execution needs to be approved by this admin."
            + COMPANY_INFORMATION
            + COMPLETION_PROMPT
        )
        DATA_ENGINEER_PROMPT = (
            "A Data Engineer. You follow an approved plan. Generate the initial SQL based on the requirements provided. Send it to the Sr Data Analyst to be executed."
            + "Some notes are as follows:"
            + "- The database use SQL Server, so don't use LIMIT, EXTRACT, and remember that 'Order' is a reserved word."
            + "- Alerts are grouped."
            + "- These are very large databases, do not return all records."
            #+ "- When filtering datetimes, use temporary tables to improve performance before applying additional filters."
            + "- Not all the columns are indexed, so for datetime queries use columns such as DateTimeAlarmClosed and is recorded in UTC."
            + "- To get SLAs we use Group alarms."
            + "- SLA for response time (ResponseTime column) is 30 seconds."
            + "- CARS standards for 'Cratos Alarm Reduction System'."
            + "- When asked for volumes of alerts, we should be querying the individual alerts rather than group alerts."
            + "- Group Alerts has multiple alerts in that group. You should consider the count of alerts in each group or individual alerts when asked about number of alerts."
            + "- Always exclude alarms with alarm type equal to 'ViewSiteInformation', 'ArchivePlayback', 'UserCall'."
            + "- When asked for the last month, take that to mean the previous calendar month. Same for weeks."
            + "- 'IsSoftwareHandled' or 'ClosedbyCARS' means that the alarm is handled by a computer (software) rather than a human."
            + "- CARS is not a user (or specialist as we call them), it is a system."
            + "- A system is made up of multiple cameras plugged into the same NVR (Network Video Recorder, also called a Transmitter)."
            + "- A site (an office, or warehouse of example) is made up of one of more systems."
            + "- We don't care for SLAs if the site isn't live, i.e. CommissionStatus is 1 for Live."
            + "- We have 'IsCurrent' for operators, customers, sites, system, and system device (camera)."
            + "- We have IndividualAlarmsUS_Day which aggregates alerts and SLAs into days which can be more efficient to query."
            + "- We have GroupAlarmsUS_Day which aggregates group alarms and SLAs into days which can be more efficient to query."
            + "- When asked about users, or specialists, consider this is IsHuman is true (i.e. not handled by software or CARS)."
            + "- Sites have reference codes that are used in other systems such as Salesforce."
            + "- We use reference codes of the format s**-****-xxxx ('s' for site, then 'us' for US, or 'c' for customer, or 'o' for operator)."
            + "- A customer can have multiple sites."
            + "- A dealer can have multiple customers."
            + "- All dealers are operators, but not all operators are dealers."
            + "- A hub is a team of people that alerts."
            + "- An event is an alarm or an alert."
            + "- Cams is the name of the application that users use."
            + "- An incident is an alarm (or alarms) of interest."
            + "- An isolation is instruction for our software to handle the alarm instead of a human."
            + "- Tables are often suffixed with their region, e.g. 'US', 'EMEA'."
            + COMPANY_INFORMATION
            + COMPLETION_PROMPT
        )
        SR_DATA_ANALYST_PROMPT = (
            "Sr Data Analyst. You follow an approved plan. You run the SQL query, generate the response, summarize the results, and send it to the product manager for final review."
            + "Some things to consider are:"
            + "- Users are also known as specialists."
            + "- An event is an alarm or an alert."
            + "- An incident is an alarm (or alarms) of interest."
            + "- An isolation is instruction for our software to handle the alarm instead of a human."
            + "- We don't care for SLAs if the site isn't live, i.e. CommissionStatus is 1 for Live."
            + "- 'CARS - EMLI - Incoming' is not a 'user', it is a computer system."
            + "- Cams is the name of the application that users use."
            + "- Users with [Dev] are IT users and might be doing operational work on the system and you should consider ignoring their results."
            + COMPANY_INFORMATION
            #+ COMPLETION_PROMPT
        )
        PRODUCT_MANAGER_PROMPT = (
            "Product Manager. Validate the response to make sure it's correct"
            + COMPANY_INFORMATION
            + COMPLETION_PROMPT
        )

        # create a set of agents with specific roles
        # admin user proxy agent - takes in the prompt and manages the group chat
        user_proxy = autogen.UserProxyAgent(
            name="Admin",
            #llm_config=azureai_config,
            system_message=USER_PROXY_PROMPT,
            code_execution_config=False,#{"work_dir": "web"},
            human_input_mode="ALWAYS",
            is_termination_msg=is_termination_msg,
        )

        # data engineer agent - generates the sql query
        data_engineer = autogen.AssistantAgent(
            name="Engineer",
            llm_config=azureai_config,
            system_message=DATA_ENGINEER_PROMPT,
            code_execution_config=False,
            human_input_mode="NEVER",
            is_termination_msg=is_termination_msg,
        )

        # sr data analyst agent - run the sql query and generate the response
        sr_data_analyst = autogen.AssistantAgent(
            name="Sr_Data_Analyst",
            llm_config=azureai_config,
            system_message=SR_DATA_ANALYST_PROMPT,
            code_execution_config=False,
            human_input_mode="NEVER",
            is_termination_msg=is_termination_msg,
            function_map=function_map,
        )

        # product manager - validate the response to make sure it's correct
        product_manager = autogen.AssistantAgent(
            name="Product_Manager",
            llm_config=azureai_config,
            system_message=PRODUCT_MANAGER_PROMPT,
            code_execution_config=False,
            human_input_mode="NEVER",
            is_termination_msg=is_termination_msg,
        )

        # create a group chat and initiate the chat.
        groupchat = autogen.GroupChat(
            agents=[user_proxy, data_engineer, sr_data_analyst, product_manager],
            messages=[],
            max_round=20,
        )
        manager = autogen.GroupChatManager(groupchat=groupchat, llm_config=azureai_config)

        user_proxy.initiate_chat(manager, clear_history=True, message=prompt)


if __name__ == "__main__":
    main()
