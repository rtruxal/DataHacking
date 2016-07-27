import csv

from counter import Counter
from pprint import pprint

def group_by_country(list_of_dicts):
    result = {}
    for input_dict in list_of_dicts:
        cc = input_dict['Country Code']
        if cc not in result.keys():
            result.update({cc : {input_dict['Indicator Name'] : input_dict}})
        else:
            result[cc].update({input_dict['Indicator Name'] : input_dict})
    return result
# foo = [{'a' : 2, 'b' : 4, 'Country Code' : 'USA', 'Indicator Name' : 'foo'}, {'a' : 3, 'b' : 10, 'Country Code' : 'RUS', 'Indicator Name' : 'foo2'}, {'a' : 1, 'c' : 4, 'Country Code' : 'USA', 'Indicator Name' : 'foo3'}]
# group_by_country(foo)



def count_tags(filename_str):
    with open(filename_str, 'rb') as csvfile:
        records = list(csv.DictReader(csvfile))
        count = Counter()
        for rec in records:
            count.update({rec['Indicator Name'] : 1})
        pprint(count.keys())

#count_tags('WDI_Data.csv')

def read_csvdata(filename_str):
    with open(filename_str, 'rb') as csvfile:
        records = list(csv.DictReader(csvfile))
        # print type(records), type(records[0])
        new_mess = group_by_country(records)
        # new_mess IS {COUNTRY KEY : INQUIRY TYPE KEY : {DATA}}}
        return new_mess

def read_in_ccds(filename_str):
    with open(filename_str, 'rb') as csvfile:
        result_dict = {}
        a3 = 'Alpha-3 code'
        full_name = 'English short name lower case'
        list_of_dicts = list(csv.DictReader(csvfile))
        for dict_rec in list_of_dicts:
            result_dict.update({dict_rec[a3] : dict_rec[full_name]})
        return result_dict


def amurrica(three_layer_dict):
    for key, value in three_layer_dict.items():
        if key == 'USA':
            return value
        else: pass


def select_years_from_keys(one_layer_dict, return_meta_data=False):
    import re
    from collections import OrderedDict
    result_dict = OrderedDict()
    metadata = {}
    for key, value in one_layer_dict.items():
        if re.findall(r'^\d\d\d\d$', key):
            result_dict[key] = value
        elif return_meta_data:
            metadata[key] = value
    result_dict = OrderedDict(sorted(result_dict.iteritems(), key=lambda x : x[0]))
    if return_meta_data:
        metadata = OrderedDict(sorted(metadata.iteritems(), key=lambda x : x[0]))
        return result_dict, metadata
    else:
        return result_dict

def extract_data_for_dicts_in_dict(two_layer_dict, topic_string_list, return_meta_data=False):
    year_data = {}
    metadata = {}
    if return_meta_data:
        for topic in topic_string_list:
            temp_rec, meta = select_years_from_keys(two_layer_dict[topic], return_meta_data=return_meta_data)
            year_data[topic] = temp_rec
            metadata[topic] = meta
        return year_data, metadata
    else:
        for topic in topic_string_list:
            temp_rec = select_years_from_keys(two_layer_dict[topic])
            year_data[topic] = temp_rec
        return year_data


def turn_yeardicts_into_dataframe(two_layer_dict):
    import pandas as pd
    keys = []
    counter = 0
    for data in two_layer_dict.values():
        if counter >= 1:
            break
        keys = data.keys()
    data_frame = pd.DataFrame(columns=keys, index=two_layer_dict.keys())
    for key, value in two_layer_dict.items():
        data_frame.loc[key] = pd.Series(value)
    return data_frame

def write_to_csv(panda_data, new_file_name_str):
    panda_data.to_csv(new_file_name_str)
    print 'file: {} created'.format(new_file_name_str)


def print_labels():
    three_layer_dict = read_csvdata('WDI_Data.csv')
    american_data = amurrica(three_layer_dict)
    print american_data.keys()

def main():
    from variables import topic_strings
    three_layer_dict = read_csvdata('WDI_Data.csv')
    # extract_count_of_blanks(three_layer_dict)
    american_data = amurrica(three_layer_dict)
    data_by_year, metadata = extract_data_for_dicts_in_dict(american_data, topic_strings, return_meta_data=True)
    # print data_by_year, metadata
    data_frame = turn_yeardicts_into_dataframe(data_by_year)
    write_to_csv(data_frame, 'American_econ_data_1.csv')
    # cc means country code
    # ccode_cname_dict = read_in_ccds('country_code_data.csv')

if __name__ == '__main__':
    main()





