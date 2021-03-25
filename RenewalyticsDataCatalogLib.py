from google.cloud import datacatalog_v1
from google.api_core.exceptions import NotFound, PermissionDenied
from datetime import datetime
import logging

# TODO - Create python package


logging.getLogger().setLevel(logging.INFO)


def get_tag_template(datacatalog, project_id, location, tag_template_name):
    logging.debug('get_tag_template(...) \n\tproject_id={}\n\tlocation={}\n\ttag_template_name={}'
          .format(project_id, location, tag_template_name))

    tag_template_path = datacatalog.tag_template_path(project=project_id, location=location,
                                                      tag_template=tag_template_name)
    logging.debug('get_tag_template(...) \n\ttag_template_path={}'.format(tag_template_path))

    try:
        tag_template = datacatalog.get_tag_template(name=tag_template_path)
    except (NotFound, PermissionDenied):
        tag_template_display_name = get_tag_template_display_name(tag_template_name)
        tag_template = datacatalog_v1.types.TagTemplate()
        tag_template.display_name = tag_template_display_name

        tag_template.fields['code_module'] = datacatalog_v1.types.TagTemplateField()
        tag_template.fields['code_module'].display_name = 'Code module'
        tag_template.fields['code_module'].type_.primitive_type = datacatalog_v1\
            .types.FieldType.PrimitiveType.STRING

        tag_template.fields['language'] = datacatalog_v1.types.TagTemplateField()
        tag_template.fields['language'].display_name = 'Language'
        tag_template.fields['language'].type_.primitive_type = datacatalog_v1\
            .types.FieldType.PrimitiveType.STRING

        tag_template.fields['create_datetime'] = datacatalog_v1.types.TagTemplateField()
        tag_template.fields['create_datetime'].display_name = 'Datetime created'
        tag_template.fields['create_datetime'].type_.primitive_type = datacatalog_v1\
            .types.FieldType.PrimitiveType.TIMESTAMP

        tag_template = datacatalog.create_tag_template(
            parent=datacatalog_v1.DataCatalogClient.common_location_path(project_id, location),
            tag_template_id=tag_template_name,
            tag_template=tag_template)
        logging.debug('Created template: {}'.format(tag_template.name))
    else:
        logging.debug('get_tag_template(...) found {}'.format(tag_template.name))
    return tag_template


def get_tag_template_display_name(tag_template_name: str):
    if tag_template_name == 'data_import':
        return 'Data Import'
    else:
        raise('get_tag_template_diaplsy_name ERROR - unsupported tag_template_name {}'.format(tag_template_name))


def get_entry_group(datacatalog, project_id, location, entry_group_id):

    expected_entry_group_name = datacatalog_v1.DataCatalogClient \
        .entry_group_path(project_id, location, entry_group_id)
    try:
        entry_group = datacatalog.get_entry_group(name=expected_entry_group_name)
    except (NotFound, PermissionDenied):
        entry_group_obj = datacatalog_v1.types.EntryGroup()
        entry_group_obj.display_name = 'Renewalytics Entry Group'
        entry_group_obj.description = 'Renewalytics Entry Group description'
        entry_group = datacatalog.create_entry_group(
            parent=datacatalog_v1.DataCatalogClient.common_location_path(project_id, location),
            entry_group_id=entry_group_id,
            entry_group=entry_group_obj)
    else:
        logging.debug('get_entry_group(...) found {}'.format(entry_group.name))
    return entry_group


def get_entry(datacatalog, project_id, location,
              entry_group, entry_id, entry_display_name,
              user_specified_type, user_specified_system, dataset_id, table_id):
    logging.debug('get_entry(...) \n\tproject_id={}\n\tlocation={}\n\tentry_group.name={}\n\tentry_id={}'
          .format(project_id, location, entry_group.name, entry_id))
    entry_group_id = entry_group.name.split('/')[-1]
    logging.debug('get_entry(...) entry_group_id={}'.format(entry_group_id))
    expected_entry_name = datacatalog_v1.DataCatalogClient \
        .entry_path(project_id, location, entry_group_id, entry_id)
    logging.debug('get_entry(...) expected name {}'.format(expected_entry_name))
    try:
        entry = datacatalog.get_entry(name=expected_entry_name)
    except (NotFound, PermissionDenied):
        entry = datacatalog_v1.types.Entry()
        entry.linked_resource = '//bigquery.googleapis.com/projects/{}/datasets/{}/tables/{}' \
            .format(project_id, dataset_id, table_id)
        entry.display_name = entry_display_name
        entry.user_specified_type = user_specified_type
        entry.user_specified_system = user_specified_system
        entry = datacatalog.create_entry(
            parent=entry_group.name,
            entry_id=entry_id,
            entry=entry)
    else:
        logging.debug('get_entry(...) found {}'.format(entry.name))
    return entry


def delete_entry(datacatalog, project_id, location, entry_group_id, entry_id):
    # Not working
    expected_entry_name = datacatalog_v1.DataCatalogClient \
        .entry_path(project_id, location, entry_group_id, entry_id)
    logging.debug('delete_entry_group(...): removing {}'.format(expected_entry_name))
    try:
        datacatalog.delete_entry(name=expected_entry_name)
    except (NotFound, PermissionDenied):
        logging.debug('delete_entry_group(...): NotFound, PermissionDenied')
        #pass
    else:
        logging.debug('delete_entry_group(...): removed {}'.format(expected_entry_name))


def delete_entry_group(datacatalog, project_id, location, entry_group_id):
    # Not working
    #expected_entry_group_name = datacatalog_v1.DataCatalogClient \
    expected_entry_group_name = datacatalog.entry_group_path(project_id, location, entry_group_id)
    print('delete_entry_group(...): removing {}'.format(expected_entry_group_name))
    #datacatalog.delete_entry_group(name=expected_entry_group_name)
    # try:
    #     datacatalog.delete_entry_group(name=expected_entry_group_name)
    # except (NotFound, PermissionDenied):
    #     print('delete_entry_group(...): NotFound, PermissionDenied')
    #     #pass
    # else:
    #     print('delete_entry_group(...): removed {}'.format(expected_entry_group_name))


def get_tag_if_exists_in_catalog(datacatalog, entry, metadata):
    tag_exists = False
    for (k, v) in metadata.items():
        for t in datacatalog.list_tags(parent=entry.name):
            for f in t.fields:
                if f == k:
                    logging.debug('tag_exists_in_catalog(...) Found existing tag {}'.format(k))
                    return t
    logging.debug('tag_exists_in_catalog(...) tag_exists = {}'.format(tag_exists))
    return None


def set_tag(datacatalog: datacatalog_v1.DataCatalogClient,
            tag_template, entry, metadata):
    logging.debug('set_tag(...) \n\ttag_template.name={}\n\tentry.name={}\n\tmetadata={}'
          .format(tag_template.name, entry.name, metadata))

    tag = get_tag_if_exists_in_catalog(datacatalog, entry, metadata)
    if tag is None:
        tag = datacatalog_v1.types.Tag()
        tag_exists = False
    else:
        tag_exists = True

    if not tag_exists:
        tag = datacatalog_v1.types.Tag()
        tag.template = tag_template.name

    for (k, v) in metadata.items():
        if k not in tag.fields:
            tag.fields[k] = datacatalog_v1.types.TagField()
        if type("") == type(v):
            tag.fields[k].string_value = v
        elif type(1.0) == type(v):
            tag.fields[k].double_value = v
        elif type(1) == type(v):
            tag.fields[k].double_value = v
        elif type(datetime.now()) == type(v):
            tag.fields[k].timestamp_value = v
        else:
            raise TypeError("type error in set_tag(...): Unsupported type {}".format(type(v)))

    if tag_exists:
        datacatalog.update_tag(tag=tag)
    else:
        tag = datacatalog.create_tag(parent=entry.name, tag=tag)
    logging.info('Created tag: {}'.format(tag.name))
    return tag


def set_metadata(datacatalog, project_id, location, tag_template_name, dataset_id, table_id, metadata):
    logging.debug('set_metadata(...): {}'.format(metadata))
    entry_group_id = dataset_id
    entry_id = 'renewalytics_entry_{}_{}'.format(dataset_id,table_id)
    entry_display_name = '{}_{}'.format(dataset_id, table_id)
    user_specified_type = 'Table'
    user_specified_system = 'BigQuery'

    tag_template = get_tag_template(datacatalog, project_id, location, tag_template_name)

    entry = get_entry(datacatalog, project_id, location,
                      get_entry_group(datacatalog, project_id, location, entry_group_id),
                      entry_id, entry_display_name, user_specified_type, user_specified_system, dataset_id, table_id)
    tag = set_tag(datacatalog, tag_template, entry, metadata)
    return tag
