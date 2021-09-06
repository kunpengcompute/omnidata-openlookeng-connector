/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package com.huawei.boostkit.omnidata;

/**
 * Property for omniData communication
 *
 * @since 2021-08-27
 */
public class OmniDataProperty {
    private OmniDataProperty() {}

    /**
     * constant string for "grpc.client.target.list"
     */
    public static final String GRPC_CLIENT_TARGET_LIST = "grpc.client.target.list";

    /**
     * delimiter string for "grpc.client.target.list"
     * examples: 192.0.2.1:80,192.0.2.2:80
     */
    public static final String HOSTADDRESS_DELIMITER = ",";

    /**
     * constant string for "grpc.client.target"
     */
    public static final String GRPC_CLIENT_TARGET = "grpc.client.target";

    /**
     * constant string for "grpc.ssl.enabled"
     */
    public static final String GRPC_SSL_ENABLED = "grpc.ssl.enabled";

    /**
     * constant String for "pki.dir"
     */
    public static final String PKI_DIR = "pki.dir";
}

