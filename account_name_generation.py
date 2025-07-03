from db_connection import get_snowflake_connection
from open_ai_interactions import get_openai_client_obj, interact_with_chat_application, interact_with_gpt4
import pandas as pd


def main():
    conn = get_snowflake_connection()
    portfolio_general_info = conn.cursor().execute("SELECT PORTFOLIOCODE, NAME, OPENDATE, PERFORMANCEINCEPTIONDATE, TERMINATIONDATE, BASECURRENCYCODE, BASECURRENCYNAME, PRODUCTCODE FROM portfoliogeneralinformation where ProductCode is not null and PortfolioCode not in (select distinct REPRESENTATIVEACCOUNT from ProductMaster)")
    portfolio_general_info_df = pd.DataFrame.from_records(iter(portfolio_general_info), columns=[x[0] for x in portfolio_general_info.description])

    num_accounts = portfolio_general_info_df.shape[0]
    openai_client = get_openai_client_obj()

    while num_accounts > 0:
        account_name_request = f"You are gnerating syntheic data for an asset management firm. Generate 200 pipe (|) separated names appropriate for separately managed accounts and do not include numbering. These should be names of public companies, pension funds, non-profit organizations and private trusts. Include the name of the account type in the name. For example, 'Apple Inc. Pension Fund' or 'Microsoft Corporation Trust'."
        message_text = [{"role": "system", "content": "you are a synthetic data generator for an asset management firm. do not include any text other than the required answer and act as a completion service"},{"role": "user", "content": account_name_request}]
        content = interact_with_gpt4(message_text, openai_client, use_mini_model=True)['choices'][0]['message']['content']
        account_name_list = content.split('|')

        for account_name in account_name_list:
            account_name = account_name.strip()
            portfolio_general_info_df.loc[num_accounts-1, 'NAME'] = account_name
            num_accounts -= 1
            if num_accounts == 0:
                break

    portfolio_general_info_df.to_csv('account_names.csv', index=False, sep="|")

if __name__ == "__main__":
    main() 

       