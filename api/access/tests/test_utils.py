import pytest
import unittest
from unittest import mock
from access.utils import prepareAnomalyDataframes
from pandas import Timestamp
from decimal import Decimal
from mixer.backend.django import mixer
import pandas as pd

def test_datasets(client, mocker):
    fakedata = [{'CREATEDTS': Timestamp('2021-06-21 00:00:00+0000', tz='UTC'),
        'DeliveryCity': 'Lucknow',
        'DeliveryPostalCode': '226010',
        'RefundAmount': Decimal('0E-9'),
        'ReceivedQty': Decimal('1.000000000')},
        {'CREATEDTS': Timestamp('2021-06-21 00:00:00+0000', tz='UTC'),
        'DeliveryCity': 'Indore',
        'DeliveryPostalCode': '452016',
        'RefundAmount': Decimal('249.000000000'),
        'ReceivedQty': Decimal('1.000000000')},
        {'CREATEDTS': Timestamp('2021-06-21 00:00:00+0000', tz='UTC'),
        'DeliveryCity': 'Lucknow',
        'DeliveryPostalCode': '226028',
        'RefundAmount': Decimal('0E-9'),
        'ReceivedQty': Decimal('0E-9')},
        {'CREATEDTS': Timestamp('2021-06-21 00:00:00+0000', tz='UTC'),
        'DeliveryCity': 'Indore',
        'DeliveryPostalCode': '452018',
        'RefundAmount': Decimal('0E-9'),
        'ReceivedQty': Decimal('0E-9')},
        {'CREATEDTS': Timestamp('2021-06-21 00:00:00+0000', tz='UTC'),
        'DeliveryCity': 'Indore',
        'DeliveryPostalCode': '452001',
        'RefundAmount': Decimal('2616.000000000'),
        'ReceivedQty': Decimal('4.000000000')},
        {'CREATEDTS': Timestamp('2021-06-21 00:00:00+0000', tz='UTC'),
        'DeliveryCity': 'Lucknow',
        'DeliveryPostalCode': '226018',
        'RefundAmount': Decimal('0E-9'),
        'ReceivedQty': Decimal('0E-9')},
        {'CREATEDTS': Timestamp('2021-06-21 00:00:00+0000', tz='UTC'),
        'DeliveryCity': 'Lucknow',
        'DeliveryPostalCode': '226010',
        'RefundAmount': Decimal('2282.750000000'),
        'ReceivedQty': Decimal('4.000000000')},
        {'CREATEDTS': Timestamp('2021-06-21 00:00:00+0000', tz='UTC'),
        'DeliveryCity': 'Indore',
        'DeliveryPostalCode': '452001',
        'RefundAmount': Decimal('0E-9'),
        'ReceivedQty': Decimal('0E-9')},
        {'CREATEDTS': Timestamp('2021-06-21 00:00:00+0000', tz='UTC'),
        'DeliveryCity': 'Lucknow',
        'DeliveryPostalCode': '226004',
        'RefundAmount': Decimal('2158.400000000'),
        'ReceivedQty': Decimal('2.000000000')},
        {'CREATEDTS': Timestamp('2021-06-21 00:00:00+0000', tz='UTC'),
        'DeliveryCity': 'Lucknow',
        'DeliveryPostalCode': '226010',
        'RefundAmount': Decimal('999.000000000'),
        'ReceivedQty': Decimal('1.000000000')},
        {'CREATEDTS': Timestamp('2021-06-21 00:00:00+0000', tz='UTC'),
        'DeliveryCity': 'Lucknow',
        'DeliveryPostalCode': '226008',
        'RefundAmount': Decimal('0E-9'),
        'ReceivedQty': Decimal('1.000000000')},
        {'CREATEDTS': Timestamp('2021-06-21 00:00:00+0000', tz='UTC'),
        'DeliveryCity': 'Indore',
        'DeliveryPostalCode': '452002',
        'RefundAmount': Decimal('1399.000000000'),
        'ReceivedQty': Decimal('1.000000000')},
        {'CREATEDTS': Timestamp('2021-06-21 00:00:00+0000', tz='UTC'),
        'DeliveryCity': 'Indore',
        'DeliveryPostalCode': '452002',
        'RefundAmount': Decimal('559.300000000'),
        'ReceivedQty': Decimal('1.000000000')},
        {'CREATEDTS': Timestamp('2021-06-21 00:00:00+0000', tz='UTC'),
        'DeliveryCity': 'Lucknow',
        'DeliveryPostalCode': '226011',
        'RefundAmount': Decimal('2399.000000000'),
        'ReceivedQty': Decimal('2.000000000')},
        {'CREATEDTS': Timestamp('2021-06-21 00:00:00+0000', tz='UTC'),
        'DeliveryCity': 'Lucknow',
        'DeliveryPostalCode': '226010',
        'RefundAmount': Decimal('0E-9'),
        'ReceivedQty': Decimal('0E-9')},
        {'CREATEDTS': Timestamp('2021-06-22 00:00:00+0000', tz='UTC'),
        'DeliveryCity': 'Indore',
        'DeliveryPostalCode': '452001',
        'RefundAmount': Decimal('990.000000000'),
        'ReceivedQty': Decimal('2.000000000')},
        {'CREATEDTS': Timestamp('2021-06-22 00:00:00+0000', tz='UTC'),
        'DeliveryCity': 'Indore',
        'DeliveryPostalCode': '452001',
        'RefundAmount': Decimal('2239.200000000'),
        'ReceivedQty': Decimal('1.000000000')},
        {'CREATEDTS': Timestamp('2021-06-22 00:00:00+0000', tz='UTC'),
        'DeliveryCity': 'Lucknow',
        'DeliveryPostalCode': '226003',
        'RefundAmount': Decimal('0E-9'),
        'ReceivedQty': Decimal('2.000000000')},
        {'CREATEDTS': Timestamp('2021-06-22 00:00:00+0000', tz='UTC'),
        'DeliveryCity': 'Lucknow',
        'DeliveryPostalCode': '226004',
        'RefundAmount': Decimal('0E-9'),
        'ReceivedQty': Decimal('1.000000000')},
        {'CREATEDTS': Timestamp('2021-06-22 00:00:00+0000', tz='UTC'),
        'DeliveryCity': 'Indore',
        'DeliveryPostalCode': '452001',
        'RefundAmount': Decimal('0E-9'),
        'ReceivedQty': Decimal('2.000000000')},
        {'CREATEDTS': Timestamp('2021-06-22 00:00:00+0000', tz='UTC'),
        'DeliveryCity': 'Lucknow',
        'DeliveryPostalCode': '226005',
        'RefundAmount': Decimal('0E-9'),
        'ReceivedQty': Decimal('4.000000000')},
        {'CREATEDTS': Timestamp('2021-06-22 00:00:00+0000', tz='UTC'),
        'DeliveryCity': 'Lucknow',
        'DeliveryPostalCode': '226010',
        'RefundAmount': Decimal('0E-9'),
        'ReceivedQty': Decimal('3.000000000')},
        {'CREATEDTS': Timestamp('2021-06-22 00:00:00+0000', tz='UTC'),
        'DeliveryCity': 'Lucknow',
        'DeliveryPostalCode': '226010',
        'RefundAmount': Decimal('1499.400000000'),
        'ReceivedQty': Decimal('1.000000000')},
        {'CREATEDTS': Timestamp('2021-06-22 00:00:00+0000', tz='UTC'),
        'DeliveryCity': 'Indore',
        'DeliveryPostalCode': '452010',
        'RefundAmount': Decimal('0E-9'),
        'ReceivedQty': Decimal('0E-9')},
        {'CREATEDTS': Timestamp('2021-06-22 00:00:00+0000', tz='UTC'),
        'DeliveryCity': 'Indore',
        'DeliveryPostalCode': '452010',
        'RefundAmount': Decimal('0E-9'),
        'ReceivedQty': Decimal('3.000000000')},
        {'CREATEDTS': Timestamp('2021-06-22 00:00:00+0000', tz='UTC'),
        'DeliveryCity': 'Indore',
        'DeliveryPostalCode': '452001',
        'RefundAmount': Decimal('0E-9'),
        'ReceivedQty': Decimal('3.000000000')},
        {'CREATEDTS': Timestamp('2021-06-22 00:00:00+0000', tz='UTC'),
        'DeliveryCity': 'Indore',
        'DeliveryPostalCode': '452001',
        'RefundAmount': Decimal('1050.000000000'),
        'ReceivedQty': Decimal('1.000000000')},
        {'CREATEDTS': Timestamp('2021-06-22 00:00:00+0000', tz='UTC'),
        'DeliveryCity': 'Indore',
        'DeliveryPostalCode': '452001',
        'RefundAmount': Decimal('0E-9'),
        'ReceivedQty': Decimal('0E-9')},
        {'CREATEDTS': Timestamp('2021-06-22 00:00:00+0000', tz='UTC'),
        'DeliveryCity': 'Indore',
        'DeliveryPostalCode': '452001',
        'RefundAmount': Decimal('0E-9'),
        'ReceivedQty': Decimal('0E-9')},
        {'CREATEDTS': Timestamp('2021-06-22 00:00:00+0000', tz='UTC'),
        'DeliveryCity': 'Indore',
        'DeliveryPostalCode': '452001',
        'RefundAmount': Decimal('0E-9'),
        'ReceivedQty': Decimal('0E-9')}]
    
    datasetDf = pd.DataFrame(fakedata)
    
    explodedDfs = prepareAnomalyDataframes(datasetDf, timestampCol="CREATEDTS", metricCol="ReceivedQty")
    assert (explodedDfs[0].columns == ['CREATEDTS', 'ReceivedQty']).all()
    assert explodedDfs[0].iloc[0]["ReceivedQty"] == 18


    explodedDfs = prepareAnomalyDataframes(datasetDf, timestampCol="CREATEDTS", metricCol="ReceivedQty", dimensionCol="DeliveryCity")
    assert (explodedDfs[1].columns == ['CREATEDTS', 'ReceivedQty']).all()
    assert explodedDfs[1].iloc[0]["ReceivedQty"] == 7

