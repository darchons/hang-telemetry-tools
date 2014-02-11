import simplejson as json
import mapreduce_common
mapreduce_anr_summary = __import__("mapreduce-anr-summary")

mapreduce_common.allowed_infos = mapreduce_common.allowed_infos_bhr
mapreduce_common.allowed_dimensions = mapreduce_common.allowed_dimensions_bhr

map = mapreduce_anr_summary.map
reduce = mapreduce_anr_summary.reduce
