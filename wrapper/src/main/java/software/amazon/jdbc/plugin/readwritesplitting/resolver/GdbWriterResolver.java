/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package software.amazon.jdbc.plugin.readwritesplitting.resolver;

import java.sql.SQLException;
import java.util.logging.Logger;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.plugin.readwritesplitting.GdbSettings;
import software.amazon.jdbc.plugin.readwritesplitting.ReadWriteSplittingSQLException;
import software.amazon.jdbc.plugin.readwritesplitting.RwSplitContext;
import software.amazon.jdbc.util.Messages;

/**
 * {@link WriterResolver} for Global Database: enforces accessible-region and home-region rules,
 * and returns {@link WriterResolution#stay()} for an out-of-home-region writer when Global Write
 * Forwarding is enabled. Otherwise delegates to a topology writer resolver. Ports the legacy
 * {@code GdbReadWriteSplittingPlugin.initializeWriterConnection}.
 */
public class GdbWriterResolver implements WriterResolver {

  private static final Logger LOGGER = Logger.getLogger(GdbWriterResolver.class.getName());

  private final GdbSettings settings;
  private final WriterResolver delegate;

  public GdbWriterResolver(final GdbSettings settings, final WriterResolver delegate) {
    this.settings = settings;
    this.delegate = delegate;
  }

  @Override
  public WriterResolution resolveWriter(final RwSplitContext ctx) throws SQLException {
    final HostSpec writerHost = ctx.writerHostSpec();

    if (writerHost != null && !this.settings.isInAccessibleRegion(writerHost)) {
      throw new ReadWriteSplittingSQLException(Messages.get(
          "GdbReadWriteSplittingPlugin.writerNotInAccessibleRegion",
          new Object[] {writerHost.getHost(),
              this.settings.rdsHelper().getRdsRegion(writerHost.getHost()),
              this.settings.accessibleRegions()}));
    }

    if (this.settings.restrictWriterToHomeRegion()
        && writerHost != null
        && !this.settings.isInHomeRegion(writerHost)) {

      if (this.settings.enableGwf()) {
        LOGGER.finest(() -> Messages.get(
            "GdbReadWriteSplittingPlugin.enabledGwf",
            new Object[] {this.settings.rdsHelper().getRdsRegion(writerHost.getHost())}));
        return WriterResolution.stay();
      }

      throw new ReadWriteSplittingSQLException(Messages.get(
          "GdbReadWriteSplittingPlugin.cantConnectWriterOutOfHomeRegion",
          new Object[] {writerHost.getHost(), this.settings.homeRegion()}));
    }

    return this.delegate.resolveWriter(ctx);
  }
}
