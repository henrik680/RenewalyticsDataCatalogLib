from RenewalyticsDataCatalogLib import *


def test3():
    project_id = 'datarenewalyticsio'
    location = 'eu'
    entry_group_id = 'renewalytics_entry_group'
    datacatalog = datacatalog_v1.DataCatalogClient()
    print(get_entry_group(datacatalog, project_id, location, entry_group_id))


def test5():
    project_id = 'datarenewalyticsio'
    location = 'eu'
    entry_group_id = 'renewalytics_entry_group'
    entry_id = 'renewalytics_entry'
    user_specified_type = 'renewalytics_data_asset'
    user_specified_system = 'renewalytics_data_warehouse'
    dataset_id = 'RW'
    table_id = 'SweGridArea'
    datacatalog = datacatalog_v1.DataCatalogClient()
    print(get_entry(
        datacatalog, project_id, location,
        get_entry_group(datacatalog, project_id, location, entry_group_id),
        entry_id, user_specified_type, user_specified_system, dataset_id, table_id))


def test4():
    project_id = 'datarenewalyticsio'
    location = 'eu'
    entry_group_id = 'renewalytics_entry_group'
    datacatalog = datacatalog_v1.DataCatalogClient()
    delete_entry_group(datacatalog, project_id, location, entry_group_id)


def test6():
    project_id = 'datarenewalyticsio'
    location = 'eu'
    entry_group_id = 'renewalytics_entry_group'
    entry_id = 'renewalytics_entry'
    datacatalog = datacatalog_v1.DataCatalogClient()
    delete_entry(datacatalog, project_id, location, entry_group_id, entry_id)


def test7():
    project_id = 'datarenewalyticsio'
    location = 'eu'
    tag_template_name = 'data_import'
    tag_template_display_name = 'Data Import'
    datacatalog = datacatalog_v1.DataCatalogClient()
    get_tag_template(datacatalog, project_id, location, tag_template_name, tag_template_display_name)


def test1():
    project_id = 'datarenewalyticsio'
    dataset_id = 'RW'
    table_id = 'LcoeEiaHistory'
    location = 'eu'
    entry_group_id = 'renewalytics-entry-group'

    datacatalog = datacatalog_v1.DataCatalogClient()

    resource_name = '//bigquery.googleapis.com/projects/{}/datasets/{}/tables/{}'.format(project_id, dataset_id, table_id)
    #resource_name = 'bigquery.table.{}.{}.{}'.format(project_id, dataset_id, table_id)
    #resource_name = 'datarenewalyticsio.RW.LcoeEiaHistory'
    print(resource_name)

    print(datacatalog.lookup_entry(request={'linked_resource': resource_name}))
    #expected_entry_name = datacatalog_v1.DataCatalogClient \
    #    .entry_path(project_id, location, entry_group_id, entry_id)

    expected_entry_group_name = datacatalog_v1.DataCatalogClient \
        .entry_group_path(project_id, location, entry_group_id)


def test2():
    project_id = 'datarenewalyticsio'
    dataset_id = 'RW'
    table_id = 'SweGridArea'
    tag_template_display_name = 'Data Import'
    tag_template_name = 'data_import'
    location = 'eu'
    entry_group_id = dataset_id
    entry_id = 'renewalytics_entry_{}_{}'.format(dataset_id,table_id)
    entry_display_name = '{}_{}'.format(dataset_id, table_id)
    user_specified_type = 'Table'
    user_specified_system = 'BigQuery'


    client = datacatalog_v1.DataCatalogClient()
    print(tag_template_name)
    tag_template = get_tag_template(client, project_id, location, tag_template_name, tag_template_display_name)

    entry = get_entry(client, project_id, location,
                      get_entry_group(client, project_id, location, entry_group_id),
                      entry_id, entry_display_name, user_specified_type, user_specified_system, dataset_id, table_id)
    metadata = {"code_module": "<manual-import>", "language": "sv"}
    tag = set_tag(client, tag_template, entry, metadata)
    print('Created tag: {}'.format(tag.name))


def test2b():
    project_id = 'datarenewalyticsio'
    dataset_id = 'RW'
    table_id = 'SweGridRegion'
    tag_template_name = 'data_import'
    location = 'eu'
    metadata = {"code_module": "test", "language": "sv", "ugga": "bugga"}
    client = datacatalog_v1.DataCatalogClient()
    set_metadata(client, project_id, location, tag_template_name, dataset_id, table_id,
                 metadata)



test2b()