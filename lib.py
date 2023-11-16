import json
import re
from datetime import date, datetime

import pandas as pd
import requests
from clickupython import client
from dotenv import dotenv_values

config = dotenv_values('.env')

clickup_api_url = config['CLICKUP_API_URL']
access_token = config['ACCESS_TOKEN']

headers = {'Authorization': access_token}

c = client.ClickUpClient(accesstoken=access_token)


def parse_json_bytes(json_bytes):
    json_bytes = json_bytes.decode('utf-8')
    json_bytes = re.sub(',[ \t\r\n]+}', '}', json_bytes)

    return json.loads(json_bytes)


def get_first_day_of_current_year() -> str:
    current_year = date.today().year
    first_day = f'{current_year}-01-01'

    return first_day


def get_today_date() -> str:
    today = date.today().strftime('%Y-%m-%d')

    return today


def convert_to_unixtimestamp(date_str: str) -> int:
    dt_object = datetime.strptime(date_str, '%Y-%m-%d')
    unixtimestamp = int(dt_object.timestamp())

    return unixtimestamp


def convert_from_unixtimestamp(timestamp: str, date_format='%Y-%m-%d %H:%M:%S') -> str:
    if timestamp is None:
        return '-'

    timestamp = int(timestamp) if isinstance(timestamp, str) else timestamp

    if timestamp > 1e12:
        timestamp /= 1000

    dt_object = datetime.fromtimestamp(timestamp)
    datetime_str = dt_object.strftime(date_format)

    return datetime_str


def get_team_id(team_name='BrandBaker'):
    teams = c.get_teams()
    team_id = next(team.id for team in teams.teams if team.name == team_name)

    return team_id


def get_spaces(team_id, archived=False, as_dataframe=True):
    spaces = c.get_spaces(team_id, archived=archived).dict()['spaces']

    if len(spaces) > 0:
        if as_dataframe:
            return pd.DataFrame(spaces)[['id', 'name', 'archived']]
        else:
            return spaces

    else:
        return 'No space teams.'


def get_lists(space_id, as_dataframe=True):
    folders_ = c.get_folders(space_id).dict()['folders']

    folders = []

    if len(folders_) > 0:
        if as_dataframe:
            for f_ in folders_:
                folder = {
                    'folder_id': f_['id'],
                    'folder_name': f_['name'],
                    'folder_task_count': f_['task_count'],
                    'space_name': f_['space']['name'],
                    'space_id': f_['space']['id'],
                }

                for l_ in f_['lists']:
                    list_info = {
                        'list_id': l_['id'],
                        'list_name': l_['name'],
                        'list_task_count': l_['task_count'],
                    }

                    folders.append({**folder, **list_info})

            return pd.DataFrame(folders)
        else:
            return folders_

    else:
        return 'No folder in space.'


def get_group_members(team_id):
    res = requests.get(
        url=f'{clickup_api_url}/group',
        params={'team_id': team_id},
        headers=headers,
        timeout=60,
    )

    groups_ = parse_json_bytes(res.content)['groups']

    groups = []

    if len(groups_) > 0:
        for g_ in groups_:
            if g_['name'] != 'Camp Ranger':
                group = {
                    'id': g_['id'],
                    'name': g_['name'],
                    'handle': g_['handle'],
                    'initials': g_['initials'],
                    'created': convert_from_unixtimestamp(
                        g_['date_created'], '%Y-%m-%d'
                    ),
                }

                members_ = g_['members']

                if len(members_) > 0:
                    for m_ in members_:
                        members = {'id': m_['id'], 'username': m_['username']}

                        groups.append({**group, **members})

    return pd.DataFrame(groups)


def get_task_type(custom_fields):
    type_value = next(
        (cf['value'] for cf in custom_fields if cf['name'] == 'Type'), None
    )

    if len(custom_fields) > 0 and custom_fields[0]['type_config']['options'] is None:
        return None

    type_name = (
        custom_fields[0]['type_config']['options'][type_value]['name']
        if type_value is not None
        and 0 <= type_value < len(custom_fields[0]['type_config']['options'])
        else None
    )

    return type_name


def get_job_number(custom_fields):
    job_no = next(
        (cf['value'] for cf in custom_fields if cf['name'] == 'Job Number'), None
    )

    return job_no


def get_post_url(custom_fields):
    post_url = next(
        (cf['value'] for cf in custom_fields if cf['name'] == 'FB Post URL'), None
    )

    return post_url


def get_submit_date(custom_fields):
    submit_date = next(
        (cf['value'] for cf in custom_fields if cf['name'] == 'Submit Date'), None
    )

    return convert_from_unixtimestamp(submit_date)


def get_list_name(list_id):
    list_name = c.get_list(list_id)

    return list_name.name


def get_folder_name(folder_id):
    folder_name = c.get_folder(folder_id)

    return folder_name.name


def get_space_name(space_id):
    space_name = c.get_space(space_id)

    return space_name.name


group_members = get_group_members(get_team_id())


def get_tasks(
    list_id,
    subtasks=True,
    created_date_gt=None,
    created_date_lt=None,
    due_date_gt=None,
    due_date_lt=None,
    updated_date_gt=None,
    updated_date_lt=None,
    as_dataframe=True,
):
    created_date_gt = (
        convert_to_unixtimestamp(
            created_date_gt if created_date_gt else get_first_day_of_current_year()
        )
        * 1000
    )
    created_date_lt = (
        convert_to_unixtimestamp(created_date_lt) * 1000 if created_date_lt else None
    )
    due_date_gt = convert_to_unixtimestamp(due_date_gt) * 1000 if due_date_gt else None
    due_date_lt = convert_to_unixtimestamp(due_date_lt) * 1000 if due_date_lt else None
    updated_date_gt = (
        convert_to_unixtimestamp(updated_date_gt) * 1000 if updated_date_gt else None
    )
    updated_date_lt = (
        convert_to_unixtimestamp(updated_date_lt) * 1000 if updated_date_lt else None
    )

    tasks_ = c.get_tasks(
        list_id,
        archived=False,
        subtasks=subtasks,
        include_closed=True,
        due_date_gt=due_date_gt,
        due_date_lt=due_date_lt,
        date_created_gt=created_date_gt,
        date_created_lt=created_date_lt,
        date_updated_gt=updated_date_gt,
        date_updated_lt=updated_date_lt,
    ).dict()['tasks']

    tasks = []

    if len(tasks_) > 0:
        if as_dataframe:
            list_id = None
            list_name = None

            folder_id = None
            folder_name = None

            space_id = None
            space_name = None

            for t_ in tasks_:
                task_status = t_['status']
                task_creator = t_['creator']
                task_custom_fields = t_['custom_fields']

                # list_id = t_['list']['id'] if list_id is None else list_id

                if list_id != t_['list']['id']:
                    list_id = t_['list']['id']
                    list_name = get_list_name(list_id)

                if folder_id != t_['folder']['id']:
                    folder_id = t_['folder']['id']
                    folder_name = get_folder_name(folder_id)

                if space_id != t_['space']['id']:
                    space_id = t_['space']['id']
                    space_name = get_space_name(space_id)

                task = {
                    'task_id': t_['id'],
                    'task_name': t_['name'],
                    'task_description': t_['description'],
                    'task_type': get_task_type(task_custom_fields),
                    'task_status': task_status['status'],
                    'task_status_type': task_status['type'],
                    'task_created': convert_from_unixtimestamp(t_['date_created']),
                    'task_updated': convert_from_unixtimestamp(t_['date_updated']),
                    'task_closed': convert_from_unixtimestamp(t_['date_closed']),
                    'task_start': convert_from_unixtimestamp(t_['start_date']),
                    'task_due': convert_from_unixtimestamp(t_['due_date'], '%Y-%m-%d'),
                    'creator_id': task_creator['id'],
                    'creator': task_creator['username'],
                    'parent': t_['parent'],
                    'list_id': list_id,
                    'list_name': list_name,
                    'folder_id': folder_id,
                    'folder_name': folder_name,
                    'space_id': space_id,
                    'space_name': space_name,
                    'job_no': get_job_number(task_custom_fields),
                    'submit_date': get_submit_date(task_custom_fields),
                    'post_url': get_post_url(task_custom_fields),
                }

                if len(t_['assignees']) > 0:
                    for assignee in t_['assignees']:
                        group_info = group_members[
                            group_members['id'] == int(assignee['id'])
                        ]

                        if not group_info.empty:
                            group_name = group_info['name'].values[0]
                        else:
                            group_name = None  # or any default value

                        task = {
                            **task,
                            'assignee_id': assignee['id'],
                            'assignee': assignee['username'],
                            'group': group_name,
                        }

                        tasks.append({**task})
                else:
                    tasks.append({**task})

            return pd.DataFrame(tasks)
        else:
            return tasks_

    else:
        return None
