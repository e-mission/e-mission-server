import os
import logging
import json
import requests
import asyncio
import emcommon.util as emcu

STUDY_CONFIG = os.getenv('STUDY_CONFIG', "stage-program")
CONFIGS_URL = os.getenv('CONFIGS_URL', "https://raw.githubusercontent.com/e-mission/op-deployment-configs/main/configs/")

OP_CONFIG_EXTENSION = ".nrel-op.json"

deployment_config = None

def _get_base_config(study_config, configs_url, default):
    download_url = configs_url + study_config + OP_CONFIG_EXTENSION
    logging.debug("About to download config from %s" % download_url)
    r = requests.get(download_url)
    if r.status_code != 200:
        logging.warning(f"Unable to download study config, status code: {r.status_code}, return default")
        return default
    else:
        base_config = json.loads(r.text)
        logging.debug(f"Successfully downloaded config with version {base_config['version']} "\
            f"for {base_config['intro']['translated_text']['en']['deployment_name']} "\
            f"and data collection URL {base_config['server']['connectUrl']}")
        return base_config

async def _load_default_label_options():
    labels = await emcu.read_json_resource("label-options.default.json")
    return labels

async def _load_label_options(base_config, study_config):
    labels = {}
    if "label_options" in base_config:
        dynamic_labels_url = base_config["label_options"]
        try:
            req = await emcu.fetch_url(dynamic_labels_url)
            labels = json.loads(req.text)
            logging.info(
                "Dynamic labels download was successful for op-deployment-configs: %s",
                dynamic_labels_url,
            )
        except Exception as err:
            logging.warning("Unable to download dynamic_labels_url from %s: %s",
                dynamic_labels_url, err)
    else:
        # load default labels from e-mission-common
        # https://raw.githubusercontent.com/e-mission/e-mission-common/refs/heads/master/src/emcommon/resources/label-options.default.json
        labels = await _load_default_label_options()
        if not labels:
            logging.error("Unable to load default labels while processing: %s", study_config)
        else:
            logging.info("Using successfully loaded default labels for %s", study_config)

    base_config["label_options"] = labels

async def _load_supplemental_files(base_config, study_config, configs_url):
    tasks = []
    tasks.append(asyncio.create_task(_load_label_options(base_config, study_config)))
    # TODO: read other supplemental files (e.g. survey information or
    # publishable API keys), using await to read them in parallel
    await asyncio.gather(*tasks)

def get_deployment_config(study_config=STUDY_CONFIG, configs_url=CONFIGS_URL, default=None):
    """
    Fetch the deployment config for the specified study config. Caches the result in memory to avoid repeated downloads.

    :param study_config: Name of the study config to fetch. Defaults to the value of the STUDY_CONFIG environment variable.
    :param configs_url: URL location of the config files. Defaults to GitHub; can be overridden for testing with locally served configs.
    :param default: Default value to return if the config cannot be fetched.
    :return: Deployment config dictionary or default value.
    """
    global deployment_config

    # Return cached config if already loaded
    if deployment_config is not None:
        logging.debug("Returning cached deployment config for %s at version %s" % (study_config, deployment_config['version']))
        return deployment_config
    logging.debug("No cached deployment config for %s, downloading from server" % study_config)
    deployment_config = _get_base_config(study_config, configs_url, default)
    if deployment_config is None:
        # the default will not have any supplemental file overrides
        # and we don't want to try to read keys from a None value anyway
        return deployment_config
    # Load supplemental files (labels, surveys, etc.) asynchronously
    asyncio.run(_load_supplemental_files(deployment_config, study_config, configs_url))
    return deployment_config


def is_phone_config_outdated(user_token: str, phone_config_version: int | None, phone_app_version: str | None) -> bool | None:
    """
    Return True if the phone's deployment config version is older than the server's, False if up to date,
    or None if the version check cannot be performed.
    """
    # If the server's STUDY_CONFIG doesn't match the opcode, skip the version check.
    # This happen on stage where the server is on stage-program but some opcodes are stage-study or stage-timeuse.
    # This is not an issue on prod since 1 deployment has 1 config.
    if STUDY_CONFIG not in user_token:
        logging.warning(
            "Opcode %s does not use config %s, skipping config version check",
            user_token,
            STUDY_CONFIG,
        )
        return None

    deployment_config = get_deployment_config()
    server_config_version = deployment_config.get('version')
    server_config_min_app_version = deployment_config.get('min_app_version', '0.0.0')

    logging.debug(f"Phone config @ {phone_config_version}, server config @ {server_config_version}; phone app @ {phone_app_version}")
    if server_config_version is None:
        logging.error("Server config missing version, skipping config version check")
        return None
    
    if not phone_config_version or phone_config_version >= server_config_version:
        logging.debug("Phone config is up to date")
        return False
    
    if server_config_min_app_version and phone_app_version \
        and packaging.version.parse(phone_app_version) < packaging.version.parse(server_config_min_app_version):
        logging.debug(f"Phone app version is below min_app_version {server_config_min_app_version}, skipping config update")
        return False

    logging.debug("Phone config is outdated")
    return True
