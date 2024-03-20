from datetime import datetime
import os
import yaml


def get_config(config_path):
    with open(config_path) as fp:
        return yaml.load(fp, yaml.FullLoader)


config = get_config(os.path.join(os.path.dirname(__file__), '..', 'CONFIG.yml'))

start_date = datetime(year=2010, month=7, day=18)

DEFAULT_ELECTRICITY_PRICE = 0.05

map_start_date = '2019-09-01'
map_end_date = '2021-09-01'


class Connection:
    custom_data = 'custom_data'
    blockchain_data = 'blockchain_data'

pos_isp_associations = {
    "Amazon.com, Inc.": "Amazon",
    "Amazon Technologies Inc.": "Amazon",
    "Amazon.com": "Amazon",
    "OVH SAS": "OVH",
    "OVH Hosting": "OVH",
    "OVH Australia PTY LTD": "OVH",
    "OVH US LLC": "OVH",
    "OVH Singapore PTE. LTD": "OVH",
    "DigitalOcean, LLC": "DigitalOcean",
    "Contabo GmbH": "Contabo",
    "Contabo Inc.": "Contabo",
    "Contabo Asia Private Limited": "Contabo",
    "Contabo Inc": "Contabo",
    "Akamai Technologies": "Akamai Technologies, Inc.",
    "The Constant Company, LLC": "The Constant Company",
    "Cherry Servers": "UAB Cherry Servers",
    "DataCamp Limited": "Datacamp Limited",
    "LeaseWeb Netherlands B.V.": "LeaseWeb",
    "Leaseweb Asia Pacific pte. ltd.": "LeaseWeb",
    "Leaseweb USA, Inc.": "LeaseWeb",
    "Leaseweb DE": "LeaseWeb",
    "LeaseWeb DE": "LeaseWeb",
    "LeaseWeb Asia Pacific": "LeaseWeb",
    "Leaseweb Japan K.K.": "LeaseWeb",
    "M247 Europe Infra": "M247 Europe SRL",
    "Comcast Cable Communications": "Comcast Cable Communications, LLC",
    "Charter Communications Inc": "Charter Communications",
    "Verizon Business": "Verizon",
    "Verizon Communications": "Verizon",
    "ProXad network / Free": "Proxad / Free SAS",
    "Vodafone Kabel Deutschland": "Vodafone",
    "Vodafone Portugal": "Vodafone",
    "Vodafone Ziggo": "Vodafone",
    "Vodafone Libertel B.V.": "Vodafone",
    "Vodafone Czech Republic": "Vodafone",
    "Vodafone GmbH": "Vodafone",
    "Vodafone Ireland Limited": "Vodafone",
    "Vodafone-BB Global": "Vodafone",
    "VODAFONE": "Vodafone",
    "VODAFONE-NETWORK": "Vodafone",
    "Vodafone Australia Pty Ltd": "Vodafone",
    "Vodafone Czech Republic a.s.": "Vodafone",
    "Vodafone Ireland": "Vodafone",
    "Vodafone Limited": "Vodafone",
    "Vodafone Espana S.A.U.": "Vodafone",
    "Vodafone US Inc.": "Vodafone",
    "Frontier Communications Corporation": "Frontier Communications Solutions",
    "Frontier Communications of America, Inc.": "Frontier Communications Solutions",
    "Virgin Media Limited": "Virgin Media",
    "Virgin Media Ireland": "Virgin Media",
    "Virgin Media Business": "Virgin Media",
    "CenturyLink Communications, LLC": "CenturyLink",
    "CenturyLink Communications": "CenturyLink",
    "Swisscom (Schweiz) AG - Bluewin": "Swisscom",
    "Swisscom (Schweiz) AG": "Swisscom",
    "Swisscom (Schweiz) AG - SME/Cybernet": "Swisscom",
    "KPN B.V": "KPN B.V.",
    "KPN N.V.": "KPN B.V.",
    "KPN BV": "KPN B.V.",
    "Telstra Internet": "Telstra Corporation Limited",
    "Telia Lietuva": "Telia",
    "Telia Company AB": "Telia",
    "Telia Eesti AS": "Telia",
    "Telia Norge": "Telia",
    "Telia Eesti": "Telia",
    "Telia Stofa A/S": "Telia",
    "M1 LIMITED": "M1 Limited",
    "Alibaba Cloud LLC": "Alibaba.com LLC",
    "Alibaba (US) Technology Co., Ltd.": "Alibaba.com LLC",
    "Altibox": "Altibox AS",
    "Spark New Zealand Trading Ltd": "Spark New Zealand Trading Limited",
}