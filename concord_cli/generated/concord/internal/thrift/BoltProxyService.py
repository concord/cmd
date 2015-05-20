#
# Autogenerated by Thrift Compiler (0.9.2)
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
#
#  options string: py:json,utf8strings
#

from thrift.Thrift import TType, TMessageType, TException, TApplicationException
import concord.internal.thrift.MutableEphemeralStateService
from ttypes import *
from thrift.Thrift import TProcessor
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol, TProtocol
try:
  from thrift.protocol import fastbinary
except:
  fastbinary = None


class Iface(concord.internal.thrift.MutableEphemeralStateService.Iface):
  def registerRichStream(self, r):
    """
    Parameters:
     - r
    """
    pass

  def deregisterRichStream(self, r):
    """
    Parameters:
     - r
    """
    pass

  def dispatchRecords(self, records):
    """
    Parameters:
     - records
    """
    pass

  def registerWithScheduler(self, meta):
    """
    Parameters:
     - meta
    """
    pass

  def updateSchedulerAddress(self, e):
    """
    Parameters:
     - e
    """
    pass


class Client(concord.internal.thrift.MutableEphemeralStateService.Client, Iface):
  def __init__(self, iprot, oprot=None):
    concord.internal.thrift.MutableEphemeralStateService.Client.__init__(self, iprot, oprot)

  def registerRichStream(self, r):
    """
    Parameters:
     - r
    """
    self.send_registerRichStream(r)
    return self.recv_registerRichStream()

  def send_registerRichStream(self, r):
    self._oprot.writeMessageBegin('registerRichStream', TMessageType.CALL, self._seqid)
    args = registerRichStream_args()
    args.r = r
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()

  def recv_registerRichStream(self):
    iprot = self._iprot
    (fname, mtype, rseqid) = iprot.readMessageBegin()
    if mtype == TMessageType.EXCEPTION:
      x = TApplicationException()
      x.read(iprot)
      iprot.readMessageEnd()
      raise x
    result = registerRichStream_result()
    result.read(iprot)
    iprot.readMessageEnd()
    if result.success is not None:
      return result.success
    if result.e is not None:
      raise result.e
    raise TApplicationException(TApplicationException.MISSING_RESULT, "registerRichStream failed: unknown result");

  def deregisterRichStream(self, r):
    """
    Parameters:
     - r
    """
    self.send_deregisterRichStream(r)
    return self.recv_deregisterRichStream()

  def send_deregisterRichStream(self, r):
    self._oprot.writeMessageBegin('deregisterRichStream', TMessageType.CALL, self._seqid)
    args = deregisterRichStream_args()
    args.r = r
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()

  def recv_deregisterRichStream(self):
    iprot = self._iprot
    (fname, mtype, rseqid) = iprot.readMessageBegin()
    if mtype == TMessageType.EXCEPTION:
      x = TApplicationException()
      x.read(iprot)
      iprot.readMessageEnd()
      raise x
    result = deregisterRichStream_result()
    result.read(iprot)
    iprot.readMessageEnd()
    if result.success is not None:
      return result.success
    if result.e is not None:
      raise result.e
    raise TApplicationException(TApplicationException.MISSING_RESULT, "deregisterRichStream failed: unknown result");

  def dispatchRecords(self, records):
    """
    Parameters:
     - records
    """
    self.send_dispatchRecords(records)
    self.recv_dispatchRecords()

  def send_dispatchRecords(self, records):
    self._oprot.writeMessageBegin('dispatchRecords', TMessageType.CALL, self._seqid)
    args = dispatchRecords_args()
    args.records = records
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()

  def recv_dispatchRecords(self):
    iprot = self._iprot
    (fname, mtype, rseqid) = iprot.readMessageBegin()
    if mtype == TMessageType.EXCEPTION:
      x = TApplicationException()
      x.read(iprot)
      iprot.readMessageEnd()
      raise x
    result = dispatchRecords_result()
    result.read(iprot)
    iprot.readMessageEnd()
    if result.e is not None:
      raise result.e
    return

  def registerWithScheduler(self, meta):
    """
    Parameters:
     - meta
    """
    self.send_registerWithScheduler(meta)
    self.recv_registerWithScheduler()

  def send_registerWithScheduler(self, meta):
    self._oprot.writeMessageBegin('registerWithScheduler', TMessageType.CALL, self._seqid)
    args = registerWithScheduler_args()
    args.meta = meta
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()

  def recv_registerWithScheduler(self):
    iprot = self._iprot
    (fname, mtype, rseqid) = iprot.readMessageBegin()
    if mtype == TMessageType.EXCEPTION:
      x = TApplicationException()
      x.read(iprot)
      iprot.readMessageEnd()
      raise x
    result = registerWithScheduler_result()
    result.read(iprot)
    iprot.readMessageEnd()
    if result.e is not None:
      raise result.e
    return

  def updateSchedulerAddress(self, e):
    """
    Parameters:
     - e
    """
    self.send_updateSchedulerAddress(e)
    self.recv_updateSchedulerAddress()

  def send_updateSchedulerAddress(self, e):
    self._oprot.writeMessageBegin('updateSchedulerAddress', TMessageType.CALL, self._seqid)
    args = updateSchedulerAddress_args()
    args.e = e
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()

  def recv_updateSchedulerAddress(self):
    iprot = self._iprot
    (fname, mtype, rseqid) = iprot.readMessageBegin()
    if mtype == TMessageType.EXCEPTION:
      x = TApplicationException()
      x.read(iprot)
      iprot.readMessageEnd()
      raise x
    result = updateSchedulerAddress_result()
    result.read(iprot)
    iprot.readMessageEnd()
    if result.e is not None:
      raise result.e
    return


class Processor(concord.internal.thrift.MutableEphemeralStateService.Processor, Iface, TProcessor):
  def __init__(self, handler):
    concord.internal.thrift.MutableEphemeralStateService.Processor.__init__(self, handler)
    self._processMap["registerRichStream"] = Processor.process_registerRichStream
    self._processMap["deregisterRichStream"] = Processor.process_deregisterRichStream
    self._processMap["dispatchRecords"] = Processor.process_dispatchRecords
    self._processMap["registerWithScheduler"] = Processor.process_registerWithScheduler
    self._processMap["updateSchedulerAddress"] = Processor.process_updateSchedulerAddress

  def process(self, iprot, oprot):
    (name, type, seqid) = iprot.readMessageBegin()
    if name not in self._processMap:
      iprot.skip(TType.STRUCT)
      iprot.readMessageEnd()
      x = TApplicationException(TApplicationException.UNKNOWN_METHOD, 'Unknown function %s' % (name))
      oprot.writeMessageBegin(name, TMessageType.EXCEPTION, seqid)
      x.write(oprot)
      oprot.writeMessageEnd()
      oprot.trans.flush()
      return
    else:
      self._processMap[name](self, seqid, iprot, oprot)
    return True

  def process_registerRichStream(self, seqid, iprot, oprot):
    args = registerRichStream_args()
    args.read(iprot)
    iprot.readMessageEnd()
    result = registerRichStream_result()
    try:
      result.success = self._handler.registerRichStream(args.r)
    except BoltError, e:
      result.e = e
    oprot.writeMessageBegin("registerRichStream", TMessageType.REPLY, seqid)
    result.write(oprot)
    oprot.writeMessageEnd()
    oprot.trans.flush()

  def process_deregisterRichStream(self, seqid, iprot, oprot):
    args = deregisterRichStream_args()
    args.read(iprot)
    iprot.readMessageEnd()
    result = deregisterRichStream_result()
    try:
      result.success = self._handler.deregisterRichStream(args.r)
    except BoltError, e:
      result.e = e
    oprot.writeMessageBegin("deregisterRichStream", TMessageType.REPLY, seqid)
    result.write(oprot)
    oprot.writeMessageEnd()
    oprot.trans.flush()

  def process_dispatchRecords(self, seqid, iprot, oprot):
    args = dispatchRecords_args()
    args.read(iprot)
    iprot.readMessageEnd()
    result = dispatchRecords_result()
    try:
      self._handler.dispatchRecords(args.records)
    except BoltError, e:
      result.e = e
    oprot.writeMessageBegin("dispatchRecords", TMessageType.REPLY, seqid)
    result.write(oprot)
    oprot.writeMessageEnd()
    oprot.trans.flush()

  def process_registerWithScheduler(self, seqid, iprot, oprot):
    args = registerWithScheduler_args()
    args.read(iprot)
    iprot.readMessageEnd()
    result = registerWithScheduler_result()
    try:
      self._handler.registerWithScheduler(args.meta)
    except BoltError, e:
      result.e = e
    oprot.writeMessageBegin("registerWithScheduler", TMessageType.REPLY, seqid)
    result.write(oprot)
    oprot.writeMessageEnd()
    oprot.trans.flush()

  def process_updateSchedulerAddress(self, seqid, iprot, oprot):
    args = updateSchedulerAddress_args()
    args.read(iprot)
    iprot.readMessageEnd()
    result = updateSchedulerAddress_result()
    try:
      self._handler.updateSchedulerAddress(args.e)
    except BoltError, e:
      result.e = e
    oprot.writeMessageBegin("updateSchedulerAddress", TMessageType.REPLY, seqid)
    result.write(oprot)
    oprot.writeMessageEnd()
    oprot.trans.flush()


# HELPER FUNCTIONS AND STRUCTURES

class registerRichStream_args:
  """
  Attributes:
   - r
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRUCT, 'r', (RichStream, RichStream.thrift_spec), None, ), # 1
  )

  def __init__(self, r=None,):
    self.r = r

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRUCT:
          self.r = RichStream()
          self.r.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('registerRichStream_args')
    if self.r is not None:
      oprot.writeFieldBegin('r', TType.STRUCT, 1)
      self.r.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __hash__(self):
    value = 17
    value = (value * 31) ^ hash(self.r)
    return value

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class registerRichStream_result:
  """
  Attributes:
   - success
   - e
  """

  thrift_spec = (
    (0, TType.BOOL, 'success', None, None, ), # 0
    (1, TType.STRUCT, 'e', (BoltError, BoltError.thrift_spec), None, ), # 1
  )

  def __init__(self, success=None, e=None,):
    self.success = success
    self.e = e

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 0:
        if ftype == TType.BOOL:
          self.success = iprot.readBool();
        else:
          iprot.skip(ftype)
      elif fid == 1:
        if ftype == TType.STRUCT:
          self.e = BoltError()
          self.e.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('registerRichStream_result')
    if self.success is not None:
      oprot.writeFieldBegin('success', TType.BOOL, 0)
      oprot.writeBool(self.success)
      oprot.writeFieldEnd()
    if self.e is not None:
      oprot.writeFieldBegin('e', TType.STRUCT, 1)
      self.e.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __hash__(self):
    value = 17
    value = (value * 31) ^ hash(self.success)
    value = (value * 31) ^ hash(self.e)
    return value

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class deregisterRichStream_args:
  """
  Attributes:
   - r
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRUCT, 'r', (RichStream, RichStream.thrift_spec), None, ), # 1
  )

  def __init__(self, r=None,):
    self.r = r

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRUCT:
          self.r = RichStream()
          self.r.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('deregisterRichStream_args')
    if self.r is not None:
      oprot.writeFieldBegin('r', TType.STRUCT, 1)
      self.r.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __hash__(self):
    value = 17
    value = (value * 31) ^ hash(self.r)
    return value

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class deregisterRichStream_result:
  """
  Attributes:
   - success
   - e
  """

  thrift_spec = (
    (0, TType.BOOL, 'success', None, None, ), # 0
    (1, TType.STRUCT, 'e', (BoltError, BoltError.thrift_spec), None, ), # 1
  )

  def __init__(self, success=None, e=None,):
    self.success = success
    self.e = e

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 0:
        if ftype == TType.BOOL:
          self.success = iprot.readBool();
        else:
          iprot.skip(ftype)
      elif fid == 1:
        if ftype == TType.STRUCT:
          self.e = BoltError()
          self.e.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('deregisterRichStream_result')
    if self.success is not None:
      oprot.writeFieldBegin('success', TType.BOOL, 0)
      oprot.writeBool(self.success)
      oprot.writeFieldEnd()
    if self.e is not None:
      oprot.writeFieldBegin('e', TType.STRUCT, 1)
      self.e.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __hash__(self):
    value = 17
    value = (value * 31) ^ hash(self.success)
    value = (value * 31) ^ hash(self.e)
    return value

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class dispatchRecords_args:
  """
  Attributes:
   - records
  """

  thrift_spec = (
    None, # 0
    (1, TType.LIST, 'records', (TType.STRUCT,(Record, Record.thrift_spec)), None, ), # 1
  )

  def __init__(self, records=None,):
    self.records = records

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.LIST:
          self.records = []
          (_etype98, _size95) = iprot.readListBegin()
          for _i99 in xrange(_size95):
            _elem100 = Record()
            _elem100.read(iprot)
            self.records.append(_elem100)
          iprot.readListEnd()
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('dispatchRecords_args')
    if self.records is not None:
      oprot.writeFieldBegin('records', TType.LIST, 1)
      oprot.writeListBegin(TType.STRUCT, len(self.records))
      for iter101 in self.records:
        iter101.write(oprot)
      oprot.writeListEnd()
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __hash__(self):
    value = 17
    value = (value * 31) ^ hash(self.records)
    return value

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class dispatchRecords_result:
  """
  Attributes:
   - e
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRUCT, 'e', (BoltError, BoltError.thrift_spec), None, ), # 1
  )

  def __init__(self, e=None,):
    self.e = e

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRUCT:
          self.e = BoltError()
          self.e.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('dispatchRecords_result')
    if self.e is not None:
      oprot.writeFieldBegin('e', TType.STRUCT, 1)
      self.e.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __hash__(self):
    value = 17
    value = (value * 31) ^ hash(self.e)
    return value

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class registerWithScheduler_args:
  """
  Attributes:
   - meta
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRUCT, 'meta', (ComputationMetadata, ComputationMetadata.thrift_spec), None, ), # 1
  )

  def __init__(self, meta=None,):
    self.meta = meta

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRUCT:
          self.meta = ComputationMetadata()
          self.meta.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('registerWithScheduler_args')
    if self.meta is not None:
      oprot.writeFieldBegin('meta', TType.STRUCT, 1)
      self.meta.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __hash__(self):
    value = 17
    value = (value * 31) ^ hash(self.meta)
    return value

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class registerWithScheduler_result:
  """
  Attributes:
   - e
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRUCT, 'e', (BoltError, BoltError.thrift_spec), None, ), # 1
  )

  def __init__(self, e=None,):
    self.e = e

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRUCT:
          self.e = BoltError()
          self.e.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('registerWithScheduler_result')
    if self.e is not None:
      oprot.writeFieldBegin('e', TType.STRUCT, 1)
      self.e.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __hash__(self):
    value = 17
    value = (value * 31) ^ hash(self.e)
    return value

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class updateSchedulerAddress_args:
  """
  Attributes:
   - e
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRUCT, 'e', (Endpoint, Endpoint.thrift_spec), None, ), # 1
  )

  def __init__(self, e=None,):
    self.e = e

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRUCT:
          self.e = Endpoint()
          self.e.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('updateSchedulerAddress_args')
    if self.e is not None:
      oprot.writeFieldBegin('e', TType.STRUCT, 1)
      self.e.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __hash__(self):
    value = 17
    value = (value * 31) ^ hash(self.e)
    return value

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class updateSchedulerAddress_result:
  """
  Attributes:
   - e
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRUCT, 'e', (BoltError, BoltError.thrift_spec), None, ), # 1
  )

  def __init__(self, e=None,):
    self.e = e

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRUCT:
          self.e = BoltError()
          self.e.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('updateSchedulerAddress_result')
    if self.e is not None:
      oprot.writeFieldBegin('e', TType.STRUCT, 1)
      self.e.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __hash__(self):
    value = 17
    value = (value * 31) ^ hash(self.e)
    return value

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)
