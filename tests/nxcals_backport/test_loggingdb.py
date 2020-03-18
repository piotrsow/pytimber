import pytest
import jpype


@pytest.mark.unit
class TestUnit:

    def test_timestamp(self, nxcals):
        import time

        now = time.time()
        ts_now = nxcals.toTimestamp(now)
        now2 = nxcals.fromTimestamp(ts_now, unixtime=True)
        dt_now2 = nxcals.fromTimestamp(ts_now, unixtime=False)
        assert now == now2
        assert str(ts_now)[:25] == dt_now2.strftime("%Y-%m-%d %H:%M:%S.%f")[:25]

        time_str = "2015-10-12 12:12:32.453255123"
        ta = nxcals.toTimestamp(time_str)
        assert ta.toLocaleString() == "Oct 12, 2015 12:12:32 PM"
        unix = nxcals.fromTimestamp(ta, unixtime=True)
        assert unix == 1444644752.4532552
        assert (
                time.strftime("%b %-d, %Y %-I:%M:%S %p", time.localtime(unix))
                == "Oct 12, 2015 12:12:32 PM"
        )

    @staticmethod
    def _search(nxcals, pattern):
        return nxcals.search(pattern)

    @pytest.mark.core
    def test_should_search(self, monkeypatch, nxcals):
        def mockreturn(pattern):
            _Variable = jpype.JPackage(
                "cern"
            ).nxcals.api.backport.domain.core.metadata.Variable

            var_list = []
            for i in [1, 2, 3]:
                var_list.append(_Variable.builder().variableName(pattern.replace('%', str(i))).build())
            return var_list

        monkeypatch.setattr(nxcals, "getVariables", mockreturn)
        variable_list = TestUnit._search(nxcals, 'VARIABLE%')

        assert 'VARIABLE1' in variable_list


@pytest.mark.integration
class TestIntegration:

    @pytest.mark.core
    @pytest.mark.parametrize("pattern, variable_name, count", [("HX:BETA%", "HX:BETASTAR_IP1", 4)])
    def test_search(self, nxcals, pattern, variable_name, count):
        variables = nxcals.search(pattern)
        assert variable_name in variables
        assert len(variables) == count

    class TestGet:

        @pytest.mark.core
        @pytest.mark.parametrize("t1, t2, variable, count, ts, value", [
            ("2015-05-13 12:00:00.000", "2015-05-15 00:00:00.000", "HX:FILLN", 6, 1431523684.764, 3715.0)])
        def test_get_simple(self, nxcals, t1, t2, variable, count, ts, value):
            data = nxcals.get(variable, t1, t2)

            t, v = data[variable]
            assert len(t) == count
            assert len(v) == count

            assert t[0] == ts
            assert v[0] == value

        import datetime

        @pytest.mark.parametrize("t1, t2, variable, dt",
                                 [("2015-05-13 12:00:00.000", "2015-05-15 00:00:00.000", "HX:FILLN",
                                   datetime.datetime(2015, 5, 13, 15, 28, 4, 764000))])
        def test_get_unixtime(self, nxcals, t1, t2, variable, dt):
            data = nxcals.get(variable, t1, t2, unixtime=False)
            t, v = data[variable]

            assert t[0] == dt

        @pytest.mark.parametrize("t1, t2, variable, count",
                                 [("2015-05-13 12:00:00.000", "2015-05-13 12:00:01.000",
                                   "LHC.BQBBQ.CONTINUOUS_HS.B1:ACQ_DATA_H", 4096)])
        def test_get_vectornumeric(self, nxcals, t1, t2, variable, count):
            data = nxcals.get(variable, t1, t2)

            t, v = data[variable]

            for vv in v:
                assert len(vv) == count

    def test_get_aligned(self, nxcals):
        # TODO
        assert True

    @pytest.mark.parametrize("pattern, variable, description",
                             [("%:LUMI_TOT_INST", "ATLAS:LUMI_TOT_INST",
                               "ATLAS: Total instantaneous luminosity summed over all bunches")])
    def test_getdescription(self, nxcals, pattern, variable, description):
        descriptions = nxcals.getDescription(pattern)

        assert descriptions[variable] == description

def test_getvariable(nxcals):
    t1 = "2015-05-13 12:00:00.000"
    t2 = "2015-05-15 00:00:00.000"
    t, v = nxcals.getVariable("HX:FILLN", t1, t2)

    assert len(t) == 6
    assert len(v) == 6

    assert t[0] == 1431523684.764
    assert v[0] == 3715.0


def test_get_vectorstring(nxcals):
    t1 = "2016-03-28 00:00:00.000"
    t2 = "2016-03-28 23:59:59.999"

    t, v = nxcals.getVariable("LHC.BOFSU:BPM_NAMES_H", t1, t2)
    assert v[0][123] == "BPM.16L3.B1"


def test_getscaled(nxcals):
    t1 = "2015-05-15 12:00:00.000"
    t2 = "2015-05-15 15:00:00.000"
    data = nxcals.getScaled(
        "MSC01.ZT8.107:COUNTS",
        t1,
        t2,
        scaleInterval="HOUR",
        scaleAlgorithm="SUM",
        scaleSize="1",
    )

    t, v = data["MSC01.ZT8.107:COUNTS"]

    import numpy as np

    assert (v[:4] - np.array([1174144.0, 1172213.0, 1152831.0])).sum() == 0


def test_getunit(nxcals):
    units = nxcals.getUnit("%:LUMI_TOT_INST")
    assert units["ATLAS:LUMI_TOT_INST"] == "Hz/ub"


def test_fundamentals(nxcals):
    fundamental = "CPS:%:SFTPRO%"
    t1 = "2015-05-15 12:00:00.000"
    t2 = "2015-05-15 12:01:00.000"
    t, v = nxcals.getVariable("CPS.TGM:USER", t1, t2, fundamental=fundamental)
    assert v[0] == "SFTPRO2"


def test_getstats(nxcals):
    t1 = "2016-03-01 00:00:00.000"
    t2 = "2016-04-03 00:00:00.000"

    vn = "LHC.BOFSU:EIGEN_FREQ_2_B1"
    stat = nxcals.getStats(vn, t1, t2)[vn]

    assert stat.MinTstamp == 1457962796.971
    assert stat.StandardDeviationValue == 0.00401594
