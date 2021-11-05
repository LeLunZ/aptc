from Scripts.crud import commit, group_stops

stop_dict = {}


def group():
    stop_groups = group_stops()

    for stop in stop_groups:
        if stop.group_ext_id not in stop_dict:
            stop_dict[stop.group_ext_id] = [stop]
        else:
            stop_dict[stop.group_ext_id].append(stop)

    '''
def group()
    stops = get_all_stops_with_group()

    for stop in stops:
        group_id = [int(e_id) for e_id in str(stop.group_ext_id).split(',')]
        group_id.append(stop.ext_id)
        stop.group_ext_id = ','.join(str(e_id) for e_id in sorted(set(group_id)))
    commit()
    '''
