SELECT DISTINCT gs.id AS plant_id, gs.GENERATING_STATION_NAME AS plant_name,COALESCE(f."TYPE", gst.NAME) AS fuel_type, rm.FULL_NAME AS region_name, ssm.FULL_NAME AS state_name, cm.classification AS utility_type, owner_details.owners AS owner_name, gsCap.installed_capacity AS installed_capacity, gsCap.installed_capacity AS effective_capacity

FROM

REPORTING_WEB_UI_UAT.GENERATING_STATION gs

LEFT JOIN REPORTING_WEB_UI_UAT.GENERATING_STATION_TYPE gst on gst.ID = gs.STATION_TYPE
LEFT JOIN reporting_web_ui_uat.GENERATING_UNIT gu ON gu.fk_generating_station =gs.id

LEFT JOIN REPORTING_WEB_UI_UAT.FUEL f on f.ID = gs.FUEL

LEFT JOIN REPORTING_WEB_UI_UAT.SRLDC_STATE_MASTER ssm on ssm.id = gs.LOCATION_ID

LEFT JOIN REPORTING_WEB_UI_UAT.REGION_MASTER rm ON ssm.REGION_ID = rm.ID
LEFT JOIN reporting_web_ui_uat.CLASSIFICATION_MASTER cm ON cm.ID = gs.CLASSIFICATION_ID 

LEFT JOIN (
			SELECT
				LISTAGG(own.owner_name, ',') WITHIN GROUP(
			ORDER BY
				owner_name ) AS owners,
				parent_entity_attribute_id AS element_id
			FROM
				reporting_web_ui_uat.entity_entity_reln ent_reln
			LEFT JOIN reporting_web_ui_uat.owner own ON
				own.id = ent_reln.child_entity_attribute_id
			WHERE
				ent_reln.child_entity = 'Owner'
				AND ent_reln.parent_entity = 'GENERATING_STATION'
				AND ent_reln.child_entity_attribute = 'OwnerId'
				AND ent_reln.parent_entity_attribute = 'Owner'
			GROUP BY
				parent_entity_attribute_id ) owner_details ON owner_details.element_id = gu.fk_generating_station

LEFT JOIN (

SELECT SUM(gu.INSTALLED_CAPACITY) AS installed_capacity, gu.FK_GENERATING_STATION

    FROM

    REPORTING_WEB_UI_UAT.GENERATING_UNIT gu

    GROUP BY gu.FK_GENERATING_STATION

    ) gsCap ON gsCap.FK_GENERATING_STATION = gs.ID
    
WHERE gu.active=1
ORDER BY plant_id asc