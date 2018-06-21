package com.framgia.sqsreceiver;

import io.dropwizard.Configuration;
import lombok.Data;

/**
 * @author: Linh Nguyen The
 * @email: nguyen.the.linhb@framgia.com
 * @company: Framgia
 * Copyright (c) Framgia 2018
 * Create 04/06/2018
 */
@Data
public class WebApplicationConfiguration extends Configuration {
    private String myConfig;
}
