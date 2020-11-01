package com.neurio.live.domain.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.neurio.live.domain.dto.SampleDto;
import com.neurio.live.domain.helper.CustomException.CustomNotFoundException;
import io.lettuce.core.api.reactive.RedisStringReactiveCommands;
import org.modelmapper.ModelMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.io.IOException;

@Service
public class SampleService {
    private Logger logger = LoggerFactory.getLogger(SampleService.class);

    @Autowired
    private RedisStringReactiveCommands<String, String> reactiveCommands;

    @Autowired
    private ModelMapper modelMapper;

    @Autowired
    private PikaStreamingClientService pikaStreamingClientService;

    @Autowired
    private LocationClientService locationClientService;

    private static final ObjectMapper json = new ObjectMapper();

    /**
     * Get latest sample from redis by locationId id. If find sample, then it is ESS device
     * Otherwise check cache for HEM device using device ID
     *
     * @param locationId the locationId id
     * @return a SampleDto object
     */
    public Mono<SampleDto> getLatest(String locationId) {
        Mono<String> deviceType = getDeviceTypeByLocation(locationId);

        return deviceType.flatMap(
            device -> {
                logger.info("getLatest location {} device type: {}", locationId, device);
                if (device.equals("ess")) {
                    // 1. cached ess request
                    logger.info("getEssLatest and updateStreamingRequestStatus: {}", locationId);
                    updateStreamingRequestStatus(locationId);
                    return getEssLatest(locationId);
                }
                else {
                    return Mono.error(
                        new CustomNotFoundException(
                            String.format("%s get not supported device type :%s", locationId, device)));
                }
            })
            .switchIfEmpty(getHemLatestByLocation(locationId));
    }

    /**
     * Get Telemetry by device Id: First check if location to device mapping existing
     * If existing, then initial HEM request. Other it is initial request of
     * either type
     * @param locationId
     * @return
     */
    private Mono<SampleDto> getHemLatestByLocation(String locationId ) {
        // Check Redis for location to device Map
        return getHemDeviceIdByLocation(locationId)
            .flatMap(
                deviceId -> {
                    // 2 Find device map, we can assume it is HEM device
                    logger.info("getHemLatestByLocation:: getHemLatest for deviceId: {}", deviceId);
                    return getHemLatest(deviceId);
                }
            )
            .switchIfEmpty(getInitialLatestByLocation(locationId));
    }

    /**
     * Initial request: first get location device Id. Then get initial by checking Redis
     * device id to device type map
     * @param locationId
     * @return
     */
    private Mono<SampleDto> getInitialLatestByLocation(String locationId ) {
        return locationClientService.getDeviceMetadataByLocationId(locationId)
            .flatMap( metaData -> Mono.justOrEmpty(metaData.getDeviceId()))
            .flatMap( x -> {
                //3. try to get latest for hem, if find telemetry, it is hem device
                logger.info("getInitialLatestByLocation::getInitialLatest for {}, {}", locationId, x);
                return getInitialLatest(locationId, x);
            })
            .switchIfEmpty(Mono.error(
                new CustomNotFoundException(
                    String.format("device id not found: %s", locationId))));
    }

    /**
     *  Initial request and we have location and device info.
     *  Then using device Id to check is device id to device type mapping exist
     *  in redis. If exist, then it is HEM device, using device Id get sample
     *  and return. If doesn't exist. Then it is ESS device
     *
     * @param locationId
     * @param deviceId
     * @return
     */
    private Mono<SampleDto> getInitialLatest(String locationId, String deviceId) {
        Mono<String> deviceType = getDeviceTypeByDevice(deviceId);
        return deviceType.flatMap( x -> {
            // if it is hem device, there must be a device Id to type map, but ess not
            if (x.equals("hem")) {
                putHemLocationDeviceMap(locationId, deviceId);
                return getHemLatest(deviceId);
            } else {
                return Mono.error(
                    new CustomNotFoundException(String.format("HEM device %s mapping to ESS device type", deviceId)));
            }
        })
            .switchIfEmpty(getInitialEssLatest(locationId));
    }

    private Mono<SampleDto> getHemLatest(String deviceId) {
        return reactiveCommands.get(buildAppLiveCacheKey(deviceId))
            .map( x -> MapTelemetryStrToSampleDto(x))
            .switchIfEmpty(
                Mono.error(
                    new CustomNotFoundException(
                        String.format("location id not found: %s", deviceId))));
    }

    private Mono<SampleDto> getEssLatest(String locationId) {
        return reactiveCommands.get(buildAppLiveCacheKey(locationId))
            .map( x -> MapTelemetryStrToSampleDto(x) )
            .switchIfEmpty(
                Mono.error(
                    new CustomNotFoundException(
                        String.format("location id not found: %s", locationId))));
    }

    private Mono<String> getHemDeviceIdByLocation(String locationId) {
        return reactiveCommands.get(buildLocationDeviceCacheKey(locationId));
    }

    private void putHemLocationDeviceMap(String locationId, String deviceId) {
        logger.info("putHemLocationDeviceMap {}: {}", locationId, deviceId);
        reactiveCommands.set(buildLocationDeviceCacheKey(locationId), deviceId);
    }

    /**
     * first time Ess device request, checked no device Id to type mapping
     * @param locationId
     * @return
     */
    private Mono<SampleDto> getInitialEssLatest(String locationId) {
        logger.info("getInitialEssLatest updateStreamingRequestStatus: {}", locationId);
        updateStreamingRequestStatus(locationId);
        return getEssLatest(locationId);
    }

    @Async
    private void updateStreamingRequestStatus(String locationId) {

        logger.debug("updateStreamingRequestStatus location: {}", locationId);
        pikaStreamingClientService
            .startPikaStreaming(locationId)
            .subscribe(
                result -> logger.info("Response from Pika: {}", result.toJSONString()),
                error -> logger.error("Error starting pika streaming for location: {}, {}", locationId, error.getMessage()));
    }

    private SampleDto MapTelemetryStrToSampleDto(String telemetry) {
        SampleDto sampleDto = new SampleDto();
        try {
            sampleDto = modelMapper.map(json.readTree(telemetry), SampleDto.class);
        } catch (IOException e) {
            logger.error(
                String.format(
                    "Converting string to json failed for these reasons: %s", e.getMessage()));
        }
        return sampleDto;
    }

    private String buildLocationDeviceCacheKey(String locationId) {
        return locationId.concat("_location_device_cache");
    }

    private String buildAppLiveCacheKey(String keyId) {
        return keyId.concat("_app_live_cache");
    }

    private Mono<String> getDeviceType(String uniqKey) {
        return reactiveCommands.get(uniqKey + "_device_type_cache");
    }

    private Mono<String> getDeviceTypeByLocation(String locationId) {
        return getDeviceType(locationId);
    }

    private Mono<String> getDeviceTypeByDevice(String deviceId) {
        return getDeviceType(deviceId);
    }

}
