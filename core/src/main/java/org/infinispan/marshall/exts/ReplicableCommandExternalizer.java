/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.marshall.exts;

import org.infinispan.atomic.DeltaAware;
import org.infinispan.commands.RemoteCommandsFactory;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.read.DistributedExecuteCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.io.UnsignedNumeric;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Set;

/**
 * ReplicableCommandExternalizer.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
public class ReplicableCommandExternalizer extends AbstractExternalizer<ReplicableCommand> {
   private final RemoteCommandsFactory cmdFactory;
   private final GlobalComponentRegistry globalComponentRegistry;
   private static final Log log = LogFactory.getLog(ReplicableCommandExternalizer.class);

    public ReplicableCommandExternalizer(RemoteCommandsFactory cmdFactory, GlobalComponentRegistry globalComponentRegistry) {
      this.cmdFactory = cmdFactory;
      this.globalComponentRegistry = globalComponentRegistry;
   }

   @Override
   public void writeObject(ObjectOutput output, ReplicableCommand command) throws IOException {
      log.infof("ReplicableCommandExternalizer.writeObject() 1. start writing new object for commandId=%d, parameter count=%d",
              command.getCommandId(),
              command.getParameters() != null? command.getParameters().length:0);
      writeCommandHeader(output, command);
      writeCommandParameters(output, command);
      log.infof("ReplicableCommandExternalizer.writeObject() 2. finished writing new object");
   }

   protected static void writeCommandParameters(ObjectOutput output, ReplicableCommand command) throws IOException {
      Object[] args = command.getParameters();
      int numArgs = (args == null ? 0 : args.length);
      log.infof("ReplicableCommandExternalizer.writeCommandParameters() for %d arguments",
              numArgs);
      UnsignedNumeric.writeUnsignedInt(output, numArgs);
      for (int i = 0; i < numArgs; i++) {
         Object arg = args[i];
         if (arg instanceof DeltaAware) {
            // Only write deltas so that replication can be more efficient
            DeltaAware dw = (DeltaAware) arg;
            Object theDelta = dw.delta();
            output.writeObject(theDelta);
            log.infof("ReplicableCommandExternalizer.writeCommandParameters() parameter #%d is DeltaAware, only wrote the Delta object = %s",
                     i, theDelta);

         } else {
            output.writeObject(arg);
            log.infof("ReplicableCommandExternalizer.writeCommandParameters() parameter #%d is normal object=%s",
                     i, arg);

         }
      }
   }

   protected void writeCommandHeader(ObjectOutput output, ReplicableCommand command) throws IOException {
      // To decide whether it's a core or user defined command, load them all and check
      Collection<Class<? extends ReplicableCommand>> moduleCommands = getModuleCommands();
      // Write an indexer to separate commands defined external to the
      // infinispan core module from the ones defined via module commands
      if (moduleCommands != null && moduleCommands.contains(command.getClass())) {
         output.writeByte(1);
        log.infof("ReplicableCommandExternalizer.writeCommandHeader() moduleCommands contains command (%s) class, wrote type=1",
                command.getClass().getName());

      }
      else {
         output.writeByte(0);
         log.infof("ReplicableCommandExternalizer.writeCommandHeader() moduleCommands (%s) DOES NOT contain command (%s) class, wrote type=0",
                 moduleCommands, command.getClass().getName());
      }

      output.writeShort(command.getCommandId());
      log.infof("ReplicableCommandExternalizer.writeCommandHeader() wrote (short) commandId=%d",
              command.getCommandId());
   }

   @Override
   public ReplicableCommand readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      log.infof("ReplicableCommandExternalizer.readObject() 1. started");
      byte type = input.readByte();
      log.infof("ReplicableCommandExternalizer.readObject() 2. type=%d", type);
      short methodId = input.readShort();
      log.infof("ReplicableCommandExternalizer.readObject() 3. (commandId) methodId=%d", methodId);
      Object[] args = readParameters(input);
      log.infof("ReplicableCommandExternalizer.readObject() 4. (commandId) arguments read for length=%d",
              args != null? args.length:0);
      ReplicableCommand command = cmdFactory.fromStream((byte) methodId, args, type);

      log.infof("ReplicableCommandExternalizer.readObject() 2. read commandId=%d with parameter count=%d, command class=%s ",
              command.getCommandId(),
              command.getParameters() != null? command.getParameters().length:0,
              command.getClass().getName());
      return command;
   }

   protected Object[] readParameters(ObjectInput input) throws IOException, ClassNotFoundException {
      log.infof("ReplicableCommandExternalizer.readParameters() entered 1.");
      int numArgs = UnsignedNumeric.readUnsignedInt(input);
      log.infof("ReplicableCommandExternalizer.readParameters() 2. numArgs=%d", numArgs);
      Object[] args = null;
      if (numArgs > 0) {
         log.infof("ReplicableCommandExternalizer.readParameters() 3. reading the parameters");
         args = new Object[numArgs];
         // For DeltaAware instances, nothing special to be done here.
         // Do not merge here since the cache contents are required.
         // Instead, merge in PutKeyValueCommand.perform
         for (int i = 0; i < numArgs; i++) {
             args[i] = input.readObject();
             log.infof("ReplicableCommandExternalizer.readParameters() 4. read parameter#%d of object=%s",
                     i, args[i]);
         }
      }
      log.infof("ReplicableCommandExternalizer.readParameters() done 5.");
      return args;
   }

   protected CacheRpcCommand fromStream(byte id, Object[] parameters, byte type, String cacheName) {
      return cmdFactory.fromStream(id, parameters, type, cacheName);
   }

   @Override
   public Integer getId() {
      return Ids.REPLICABLE_COMMAND;
   }

   @Override
   public Set<Class<? extends ReplicableCommand>> getTypeClasses() {
       Set<Class<? extends ReplicableCommand>> coreCommands = Util.<Class<? extends ReplicableCommand>>asSet(
            DistributedExecuteCommand.class, GetKeyValueCommand.class,
            ClearCommand.class, EvictCommand.class, ApplyDeltaCommand.class,
            InvalidateCommand.class, InvalidateL1Command.class,
            PutKeyValueCommand.class, PutMapCommand.class,
            RemoveCommand.class, ReplaceCommand.class);
      // Search only those commands that replicable and not cache specific replicable commands
      Collection<Class<? extends ReplicableCommand>> moduleCommands = globalComponentRegistry.getModuleProperties().moduleOnlyReplicableCommands();
      if (moduleCommands != null && !moduleCommands.isEmpty()) coreCommands.addAll(moduleCommands);
      return coreCommands;
   }

   private Collection<Class<? extends ReplicableCommand>> getModuleCommands() {
      return globalComponentRegistry.getModuleProperties().moduleCommands();
   }

}