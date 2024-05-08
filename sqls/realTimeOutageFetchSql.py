realTimeOutageFetchSql= '''SELECT
            min(gs.id) AS plant_id,
            gs.GENERATING_STATION_NAME AS station_name,
            sd_type.name    AS shut_down_type_name,
            sd_tag.name     AS shutdown_tag,
            sum(gu.INSTALLED_CAPACITY) AS Installed_Capacity
            
            FROM
            reporting_web_ui_uat.real_time_outage     outages
            LEFT JOIN reporting_web_ui_uat.outage_reason        reas ON reas.id = outages.reason_id
            LEFT JOIN reporting_web_ui_uat.entity_master        ent_master ON ent_master.id = outages.entity_id
            LEFT JOIN reporting_web_ui_uat.shutdown_outage_tag  sd_tag ON sd_tag.id = outages.shutdown_tag_id
            LEFT JOIN reporting_web_ui_uat.shutdown_outage_type sd_type ON sd_type.id = outages.shut_down_type
            LEFT JOIN reporting_web_ui_uat.GENERATING_UNIT gu ON gu.ID = outages.ELEMENT_ID 
            LEFT JOIN reporting_web_ui_uat.GENERATING_STATION gs ON gs.ID = gu.FK_GENERATING_STATION 
            LEFT JOIN reporting_web_ui_uat.CLASSIFICATION_MASTER cm ON cm.ID = gs.CLASSIFICATION_ID 
            LEFT JOIN REPORTING_WEB_UI_UAT.SRLDC_STATE_MASTER SSM on SSM.id = GS.LOCATION_ID
            LEFT JOIN reporting_web_ui_uat.GENERATING_STATION_TYPE gst ON gst.ID = gs.station_type
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
			
            WHERE ent_master.entity_name='GENERATING_UNIT' 
                AND
                    (TO_CHAR(outages.outage_date, 'YYYY-MM-DD') || ' ' || outages.OUTAGE_TIME) <= :targetDatetime
                AND (
                    (outages.REVIVED_DATE IS NULL)
                    OR 
                    (TO_CHAR(outages.REVIVED_DATE, 'YYYY-MM-DD') || ' ' || outages.REVIVED_TIME) >= :targetDatetime
                )
                GROUP BY gs.GENERATING_STATION_NAME, sd_type.name, sd_tag.name
                ORDER BY plant_id'''