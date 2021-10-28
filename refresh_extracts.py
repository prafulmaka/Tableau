"""Refresh Extracts on a Tableau Server"""

import argparse
import getpass
import tableauserverclient as tsc

import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


def main():

    parser = argparse.ArgumentParser(description="Refresh Extracts on Tableau Server")
    parser.add_argument('--server', '-s', required=True, help='tableau server url')
    parser.add_argument('--name', '-n', required=True, help='name of the tableau server object to be refreshed')
    parser.add_argument('--project', '-p', required=True,
                        help='tableau server project that hosts the object to be refreshed')
    parser.add_argument('--type', '-x', required=True, choices=['workbook', 'datasource'],
                        help='type of tableau server object to be refreshed')
    parser.add_argument('--token', '-t', required=True, help='personal access token name')
    parser.add_argument('--token_value', '-v', required=False, help='personal access token value')
    parser.add_argument('--site_url', '-u', default='', required=False, help='url namespace of tableau server site')

    args = parser.parse_args()
    token_secret = getpass.getpass(f'\n{args.token} Secret: ') if not args.token_value else args.token_value

    print('\n##############################')
    print('#                            #')
    print('#  Refresh Tableau Extracts  #')
    print('#                            #')
    print('##############################\n')

    print('###############')
    print('#  Arguments  #')
    print('###############')
    print(f'Server        : {args.server}')
    print(f'Object Name   : {args.name}')
    print(f'Object Type   : {args.type}')
    print(f'Project       : {args.project}')
    print(f'PAT Name      : {args.token}')
    print(f'Site URL Name : {args.site_url}')

    print('\n##################')
    print('#  Authenticate  #')
    print('##################')
    print(f'Signing into {args.server}')

    tableau_auth = tsc.PersonalAccessTokenAuth(args.token, token_secret, args.site_url)
    server = tsc.Server(args.server)
    server.add_http_options({'verify': False})
    server.use_server_version()

    with server.auth.sign_in(tableau_auth):

        print('Successfully signed into Tableau Server')

        req_option = tsc.RequestOptions()
        req_option.filter.add(tsc.Filter(tsc.RequestOptions.Field.Name,
                                         tsc.RequestOptions.Operator.Equals,
                                         args.name))

        if args.type.lower() == 'workbook':

            print('\n######################')
            print('#  Refresh Workbook  #')
            print('######################')
            print(f'Finding Workbook Luid for {args.project}/{args.name}')

            all_workbooks, pagination = server.workbooks.get(req_option)

            if not all_workbooks:
                raise LookupError(f'No workbooks found with the name {args.name}')

            found_workbook = next((workbook for workbook in all_workbooks if
                                  workbook.project_name.upper() == args.project.upper()), None)

            if not found_workbook:
                raise LookupError(f'No workbook found in project {args.project} with the name {args.name}')

            print(f'Workbook Luid is {found_workbook.id}')
            print('Triggering Extract Refresh')
            refresh_job = server.workbooks.refresh(found_workbook)
            print(f'Refresh Triggered. Job ID is {refresh_job.id}\n')

        elif args.type.lower() == 'datasource':

            print('\n########################')
            print('#  Refresh Datasource  #')
            print('########################')
            print(f'Finding Datasource Luid for {args.project}/{args.name}')

            all_datasources, pagination = server.datasources.get(req_option)

            if not all_datasources:
                raise LookupError(f'No datasource found with the name {args.name}')

            found_datasource = next((datasource for datasource in all_datasources if
                                     datasource.project_name.upper() == args.project.upper()), None)

            if not found_datasource:
                raise LookupError(f'No datasource found in project {args.project} with the name {args.name}')

            print(f'Datasource Luid is {found_datasource.id}')
            print('Triggering Extract Refresh')
            refresh_job = server.datasources.refresh(found_datasource)
            print(f'Refresh Triggered. Job ID is {refresh_job.id}\n')


if __name__ == '__main__':
    main()
