from unittest import TestCase
from unittest.mock import call, MagicMock

from src.global_variables import FACT_TABLE_NAME, AGGREGATED_TABLE_NAME
from src.main import parse_and_validate_api_data, aggregate_data


class TestPipeline(TestCase):
    """
    More tests:
        - Test single and batch upserts to postgres
        - Test fetching from trino
        - Create mechanism that resets test tables on each run, for example:
             DROP TABLE IF EXISTS test_spacex_launches;

            CREATE TABLE test_spacex_launches (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                launch_date_unix BIGINT,
                success BOOLEAN,
                details TEXT,
                payload_mass REAL,
                engine_start_time_unix BIGINT,
                launch_delay_hours INT);

            INSERT INTO test_spacex_launches (id, name, launch_date_unix, success, details, payload_mass, engine_start_time_unix, launch_delay_hours)
            VALUES
                ('sx_2020_001', 'Starlink-13', 1599004800, TRUE, 'Routine Starlink deployment, successful recovery.', 15600.0, 1599004700, 1),
                ('sx_2021_002', 'Transporter-2 Failure', 1625097600, FALSE, 'Anomaly during second stage burn, mission failed.', 3000.0, 1625097500, 0),
                ('sx_2021_003', 'Crew-3', 1636588800, TRUE, 'NASA Crew-3 mission to the ISS, successful splashdown.', 0.0, 1636588700, 0),
                ('sx_2022_004', 'Starlink-4_31', 1658448000, TRUE, 'Another batch of Starlink satellites to orbit.', 16000.0, 1658447900, 0),
                ('sx_2022_005', 'Nilesat 301', 1654732800, FALSE, 'Payload deployed but failed to reach target orbit.', 4500.0, 1654732700, 2),
                ('sx_2022_008', 'CRS-25', 1658092800, TRUE, 'Commercial Resupply Services mission to ISS.', 50.0, 1658092700, 3),
                ('sx_2023_006', 'Transporter-6', 1673625600, TRUE, 'Dedicated rideshare mission, multiple smallsats.', 2000.0, 1673625500, 4),
                ('sx_2023_007', 'USSF-67', 1673884800, FALSE, 'Pre-launch anomaly, mission scrubbed.', 0.0, NULL, 5);
    """

    def setUp(self):
        self.fact_table_name = "test_" + FACT_TABLE_NAME
        self.agg_table_name = "test_" + AGGREGATED_TABLE_NAME

    def test_parse_and_validate_api_data_no_payload_no_delay(self):
        raw_launches = {'static_fire_date_utc': None, 'static_fire_date_unix': None,
                        'rocket': '5e9d0d95eda69973a809d1ec', 'success': True, 'failures': [], 'details': None,
                        'flight_number': 187, 'name': 'Crew-5', 'date_utc': '2022-10-05T16:00:00.000Z',
                        'date_unix': 1664985600, 'id': '62dd70d5202306255024d139', 'date_precision': 'hour',
                        'date_local': '2022-10-05T12:00:00-04:00'}

        expected_launches = parse_and_validate_api_data(raw_launches)
        actual_launches = {
            'id': '62dd70d5202306255024d139',
            'name': 'Crew-5',
            'launch_date_unix': 1664985600,
            'success': True,
            'payload_mass': 0,
            'details': None,
            'engine_start_time_unix': None,
            'launch_delay_hours': 0
        }

        self.assertEqual(expected_launches, actual_launches)

    def test_parse_and_validate_api_data_with_payload_and_delay(self):
        raw_launches = {'static_fire_date_utc': '2022-10-05T12:00:00-05:00', 'static_fire_date_unix': 1664989200,
                        'rocket': '5e9d0d95eda69973a809d1ec', 'success': True, 'failures': [],
                        'details': 'Total payload mass was 130 kg blabla',
                        'flight_number': 187, 'name': 'Crew-5', 'date_utc': '2022-10-05T16:00:00.000Z',
                        'date_unix': 1664985600, 'id': '62dd70d5202306255024d139', 'date_precision': 'hour',
                        'date_local': '2022-10-05T12:00:00-04:00'}

        actual_launches = parse_and_validate_api_data(raw_launches)
        expected_launches = {
            'id': '62dd70d5202306255024d139',
            'name': 'Crew-5',
            'launch_date_unix': 1664985600,
            'success': True,
            'payload_mass': 130,
            'details': 'Total payload mass was 130 kg blabla',
            'engine_start_time_unix': 1664989200,
            'launch_delay_hours': 1
        }

        self.assertEqual(expected_launches, actual_launches)

    def test_aggregate_data(self):
        mock_postgres = MagicMock()
        mock_upsert = mock_postgres.upsert
        aggregate_data(self.fact_table_name, self.agg_table_name, mock_postgres)

        actual_calls = mock_upsert.call_args_list
        expected_calls = [call(self.agg_table_name,
                               ['aggregation_year', 'total_launches', 'total_successful_launches',
                                'average_payload_mass', 'average_delay_hours'],
                               [
                                   [2020, 1, 1, 15600.0, 1.0],
                                   [2021, 2, 1, 1500.0, 0.0],
                                   [2022, 3, 2, 6850.0, 1.6666666666666667],
                                   [2023, 2, 1, 1000.0, 4.5]
                               ], 'aggregation_year')]

        self.assertEqual(expected_calls, actual_calls)
