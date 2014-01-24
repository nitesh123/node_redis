var events = require("events"),
    util   = require("../util");

function Packet(type, size) {
    this.type = type;
    this.size = +size;
}

exports.name = "javascript";
exports.debug_mode = false;

<<<<<<< HEAD
function RedisReplyParser(options) {
  this.name = exports.name;
  this.options = options || {};
  this.reset();
  events.EventEmitter.call(this);
=======
function ReplyParser(options) {
    this.name = exports.name;
    this.options = options || { };

    this._buffer            = null;
    this._offset            = 0;
    this._encoding          = "utf-8";
    this._debug_mode        = options.debug_mode;
    this._reply_type        = null;
>>>>>>> origin/master
}

util.inherits(ReplyParser, events.EventEmitter);

exports.Parser = ReplyParser;

// Buffer.toString() is quite slow for small strings
<<<<<<< HEAD
function small_toString(buf, len) {
  var tmp = "", i;

  for (i = 0; i < len; i += 1) {
    tmp += String.fromCharCode(buf[i]);
  }
=======
function small_toString(buf, start, end) {
    var tmp = "", i;

    for (i = start; i < end; i++) {
        tmp += String.fromCharCode(buf[i]);
    }
>>>>>>> origin/master

  return tmp;
}

<<<<<<< HEAD
// Reset parser to it's original state.
RedisReplyParser.prototype.reset = function () {
  this.return_buffer = new Buffer(16384); // for holding replies, might grow
  this.return_string = "";
  this.tmp_string = ""; // for holding size fields

  this.multi_bulk_length = 0;
  this.multi_bulk_replies = null;
  this.multi_bulk_pos = 0;

  this.multi_bulk_nested_length = 0;
  this.multi_bulk_nested_replies = null;
  this.multi_bulk_nested_pos = 0;

  this.multi_array = null;
  this.multi_array_length = 0;
  this.multi_array_pos = 0;

  this.deep_nesting = false;

  this.states = {
    TYPE: 1,
    SINGLE_LINE: 2,
    MULTI_BULK_COUNT: 3,
    INTEGER_LINE: 4,
    BULK_LENGTH: 5,
    ERROR_LINE: 6,
    BULK_DATA: 7,
    UNKNOWN_TYPE: 8,
    FINAL_CR: 9,
    FINAL_LF: 10,
    MULTI_BULK_COUNT_LF: 11,
    BULK_LF: 12
  };

  this.state = this.states.TYPE;
};

RedisReplyParser.prototype.parser_error = function (message) {
  this.emit("error", message);
  this.reset();
};

RedisReplyParser.prototype.execute = function (incoming_buf) {
  var pos = 0, bd_tmp, bd_str, i, il, states = this.states;
  //, state_times = {}, start_execute = new Date(), start_switch, end_switch, old_state;
  //start_switch = new Date();

  while (pos < incoming_buf.length) {
    // old_state = this.state;
    // console.log("execute: " + this.state + ", " + pos + "/" + incoming_buf.length + ", " + String.fromCharCode(incoming_buf[pos]));

    switch (this.state) {
      case 1: // states.TYPE
        this.type = incoming_buf[pos];
        pos += 1;

        switch (this.type) {
          case 43: // +
            this.state = states.SINGLE_LINE;
            this.return_buffer.end = 0;
            this.return_string = "";
            break;
          case 42: // *
            this.state = states.MULTI_BULK_COUNT;
            this.tmp_string = "";
            break;
          case 58: // :
            this.state = states.INTEGER_LINE;
            this.return_buffer.end = 0;
            this.return_string = "";
            break;
          case 36: // $
            this.state = states.BULK_LENGTH;
            this.tmp_string = "";
            break;
          case 45: // -
            this.state = states.ERROR_LINE;
            this.return_buffer.end = 0;
            this.return_string = "";
            break;
          default:
            this.state = states.UNKNOWN_TYPE;
        }
        break;
      case 4: // states.INTEGER_LINE
        if (incoming_buf[pos] === 13) {
          this.send_reply(+small_toString(this.return_buffer, this.return_buffer.end));
          this.state = states.FINAL_LF;
        } else {
          this.return_buffer[this.return_buffer.end] = incoming_buf[pos];
          this.return_buffer.end += 1;
        }
        pos += 1;
        break;
      case 6: // states.ERROR_LINE
        if (incoming_buf[pos] === 13) {
          this.send_error(this.return_buffer.toString("ascii", 0, this.return_buffer.end));
          this.state = states.FINAL_LF;
        } else {
          this.return_buffer[this.return_buffer.end] = incoming_buf[pos];
          this.return_buffer.end += 1;
        }
        pos += 1;
        break;
      case 2: // states.SINGLE_LINE
        if (incoming_buf[pos] === 13) {
          this.send_reply(this.return_string);
          this.state = states.FINAL_LF;
        } else {
          this.return_string += String.fromCharCode(incoming_buf[pos]);
        }
        pos += 1;
        break;
      case 3: // states.MULTI_BULK_COUNT
        if (incoming_buf[pos] === 13) { // \r
          this.state = states.MULTI_BULK_COUNT_LF;
        } else {
          this.tmp_string += String.fromCharCode(incoming_buf[pos]);
        }
        pos += 1;
        break;
      case 11: // states.MULTI_BULK_COUNT_LF
        if (incoming_buf[pos] === 10) { // \n
          if (this.multi_bulk_length) { // nested multi-bulk
            this.multi_bulk_nested_length = this.multi_bulk_length;
            this.multi_bulk_nested_replies = this.multi_bulk_replies;
            this.multi_bulk_nested_pos = this.multi_bulk_pos;
          }
          this.multi_bulk_length = +this.tmp_string;
          this.multi_bulk_pos = 0;

          this.state = states.TYPE;
          if (this.multi_bulk_length < 0) {
            this.send_reply(null);
            this.multi_bulk_length = 0;
          } else if (this.multi_bulk_length === 0) {
            this.multi_bulk_pos = 0;
            this.multi_bulk_replies = null;
            this.send_reply([]);
          } else {
            this.multi_bulk_replies = new Array(this.multi_bulk_length);
            // for slowlog
            if (incoming_buf[pos + 1] === 42 && incoming_buf[pos + 2] === 52 && incoming_buf[pos + 5] === 58) {
              this.deep_nesting = true;
              this.multi_array = new Array(this.multi_bulk_length);
              this.multi_array_pos = this.multi_bulk_pos;
              this.multi_array_length = this.multi_bulk_length;
            }
          }
        } else {
          this.parser_error(new Error("didn't see LF after NL reading multi bulk count"));
          return;
        }
        pos += 1;
        break;
      case 5: // states.BULK_LENGTH
        if (incoming_buf[pos] === 13) { // \r
          this.state = states.BULK_LF;
        } else {
          this.tmp_string += String.fromCharCode(incoming_buf[pos]);
        }
        pos += 1;
        break;
      case 12: // states.BULK_LF
        if (incoming_buf[pos] === 10) { // \n
          this.bulk_length = +this.tmp_string;
          if (this.bulk_length === -1) {
            this.send_reply(null);
            this.state = states.TYPE;
          } else if (this.bulk_length === 0) {
            this.send_reply(new Buffer(""));
            this.state = states.FINAL_CR;
          } else {
            this.state = states.BULK_DATA;
            if (this.bulk_length > this.return_buffer.length) {
              if (exports.debug_mode) {
                console.log("Growing return_buffer from " + this.return_buffer.length + " to " + this.bulk_length);
              }
              this.return_buffer = new Buffer(this.bulk_length);
            }
            this.return_buffer.end = 0;
          }
        } else {
          this.parser_error(new Error("didn't see LF after NL while reading bulk length"));
          return;
        }
        pos += 1;
        break;
      case 7: // states.BULK_DATA
        this.return_buffer[this.return_buffer.end] = incoming_buf[pos];
        this.return_buffer.end += 1;
        pos += 1;
        if (this.return_buffer.end === this.bulk_length) {
          bd_tmp = new Buffer(this.bulk_length);
          // When the response is small, Buffer.copy() is a lot slower.
          if (this.bulk_length > 10) {
            this.return_buffer.copy(bd_tmp, 0, 0, this.bulk_length);
          } else {
            for (i = 0, il = this.bulk_length; i < il; i += 1) {
              bd_tmp[i] = this.return_buffer[i];
            }
          }
          this.send_reply(bd_tmp);
          this.state = states.FINAL_CR;
        }
        break;
      case 9: // states.FINAL_CR
        if (incoming_buf[pos] === 13) { // \r
          this.state = states.FINAL_LF;
          pos += 1;
        } else {
          this.parser_error(new Error("saw " + incoming_buf[pos] + " when expecting final CR"));
          return;
        }
        break;
      case 10: // states.FINAL_LF
        if (incoming_buf[pos] === 10) { // \n
          this.state = states.TYPE;
          pos += 1;
        } else {
          this.parser_error(new Error("saw " + incoming_buf[pos] + " when expecting final LF"));
          return;
        }
        break;
      default:
        this.parser_error(new Error("invalid state " + this.state));
    }
    // end_switch = new Date();
    // if (state_times[old_state] === undefined) {
    //     state_times[old_state] = 0;
    // }
    // state_times[old_state] += (end_switch - start_switch);
    // start_switch = end_switch;
  }
  // console.log("execute ran for " + (Date.now() - start_execute) + " ms, on " + incoming_buf.length + " Bytes. ");
  // Object.keys(state_times).forEach(function (state) {
  //     console.log("    " + state + ": " + state_times[state]);
  // });
};

RedisReplyParser.prototype.send_error = function (reply) {
  if (this.multi_bulk_length > 0 || this.multi_bulk_nested_length > 0) {
    // TODO - can this happen?  Seems like maybe not.
    this.add_multi_bulk_reply(reply);
  } else {
    this.emit("reply error", reply);
  }
};

RedisReplyParser.prototype.send_reply = function (reply) {
  if (this.multi_bulk_length > 0 || this.multi_bulk_nested_length > 0) {
    if (!this.options.return_buffers && Buffer.isBuffer(reply)) {
      this.add_multi_bulk_reply(reply.toString("utf8"));
    } else {
      this.add_multi_bulk_reply(reply);
    }
  } else {
    if (!this.options.return_buffers && Buffer.isBuffer(reply)) {
      this.emit("reply", reply.toString("utf8"));
    } else {
      this.emit("reply", reply);
    }
  }
};

RedisReplyParser.prototype.add_multi_bulk_reply = function (reply) {
  if (this.multi_bulk_replies) {
    this.multi_bulk_replies[this.multi_bulk_pos] = reply;
    this.multi_bulk_pos += 1;
    if (this.multi_bulk_pos < this.multi_bulk_length) {
      return;
=======
ReplyParser.prototype._parseResult = function (type) {
    var start, end, offset, packetHeader;

    if (type === 43 || type === 45) { // + or -
        // up to the delimiter
        end = this._packetEndOffset() - 1;
        start = this._offset;

        // include the delimiter
        this._offset = end + 2;

        if (end > this._buffer.length) {
            this._offset = start;
            throw new Error("too far");
        }

        if (this.options.return_buffers) {
            return this._buffer.slice(start, end);
        } else {
            if (end - start < 65536) { // completely arbitrary
                return small_toString(this._buffer, start, end);
            } else {
                return this._buffer.toString(this._encoding, start, end);
            }
        }
    } else if (type === 58) { // :
        // up to the delimiter
        end = this._packetEndOffset() - 1;
        start = this._offset;

        // include the delimiter
        this._offset = end + 2;

        if (end > this._buffer.length) {
            this._offset = start;
            throw new Error("too far");
        }

        if (this.options.return_buffers) {
            return this._buffer.slice(start, end);
        }

        // return the coerced numeric value
        return +small_toString(this._buffer, start, end);
    } else if (type === 36) { // $
        // set a rewind point, as the packet could be larger than the
        // buffer in memory
        offset = this._offset - 1;

        packetHeader = new Packet(type, this.parseHeader());

        // packets with a size of -1 are considered null
        if (packetHeader.size === -1) {
            return undefined;
        }

        end = this._offset + packetHeader.size;
        start = this._offset;

        // set the offset to after the delimiter
        this._offset = end + 2;

        if (end > this._buffer.length) {
            this._offset = offset;
            throw new Error("too far");
        }

        if (this.options.return_buffers) {
            return this._buffer.slice(start, end);
        } else {
            return this._buffer.toString(this._encoding, start, end);
        }
    } else if (type === 42) { // *
        offset = this._offset;
        packetHeader = new Packet(type, this.parseHeader());

        if (packetHeader.size > this._bytesRemaining()) {
            this._offset = offset - 1;
            return -1;
        }

        if (packetHeader.size < 0) {
            this._offset += 2;
            return null;
        }

        var reply = [ ];
        var ntype, i, res;

        offset = this._offset - 1;

        for (i = 0; i < packetHeader.size; i++) {
            ntype = this._buffer[this._offset++];

            if (this._offset > this._buffer.length) {
                throw new Error("too far");
            }
            res = this._parseResult(ntype);
            if (res === undefined) {
                res = null;
            }
            reply.push(res);
        }

        return reply;
    }
};

ReplyParser.prototype.execute = function (buffer) {
    this.append(buffer);

    var type, ret, offset;

    while (true) {
        offset = this._offset;
        try {
            // at least 4 bytes: :1\r\n
            if (this._bytesRemaining() < 4) {
                break;
            }

            type = this._buffer[this._offset++];

            if (type === 43) { // +
                ret = this._parseResult(type);

                if (ret === null) {
                    break;
                }

                this.send_reply(ret);
            } else  if (type === 45) { // -
                ret = this._parseResult(type);

                if (ret === null) {
                    break;
                }

                this.send_error(ret);
            } else if (type === 58) { // :
                ret = this._parseResult(type);

                if (ret === null) {
                    break;
                }

                this.send_reply(ret);
            } else if (type === 36) { // $
                ret = this._parseResult(type);

                if (ret === null) {
                    break;
                }

                // check the state for what is the result of
                // a -1, set it back up for a null reply
                if (ret === undefined) {
                    ret = null;
                }

                this.send_reply(ret);
            } else if (type === 42) { // *
                // set a rewind point. if a failure occurs,
                // wait for the next execute()/append() and try again
                offset = this._offset - 1;

                ret = this._parseResult(type);

                if (ret === -1) {
                    this._offset = offset;
                    break;
                }

                this.send_reply(ret);
            }
        } catch (err) {
            // catch the error (not enough data), rewind, and wait
            // for the next packet to appear
            this._offset = offset;
            break;
        }
    }
};

ReplyParser.prototype.append = function (newBuffer) {
    if (!newBuffer) {
        return;
    }

    // first run
    if (this._buffer === null) {
        this._buffer = newBuffer;

        return;
    }

    // out of data
    if (this._offset >= this._buffer.length) {
        this._buffer = newBuffer;
        this._offset = 0;

        return;
    }

    // very large packet
    // check for concat, if we have it, use it
    if (Buffer.concat !== undefined) {
        this._buffer = Buffer.concat([this._buffer.slice(this._offset), newBuffer]);
    } else {
        var remaining = this._bytesRemaining(),
            newLength = remaining + newBuffer.length,
            tmpBuffer = new Buffer(newLength);

        this._buffer.copy(tmpBuffer, 0, this._offset);
        newBuffer.copy(tmpBuffer, remaining, 0);

        this._buffer = tmpBuffer;
>>>>>>> origin/master
    }
  } else {
    this.multi_bulk_replies = reply;
  }

<<<<<<< HEAD
  if (this.multi_bulk_nested_length > 0 || (this.deep_nesting)) {
    this.multi_bulk_nested_replies[this.multi_bulk_nested_pos] = this.multi_bulk_replies;
    this.multi_bulk_nested_pos += 1;

    this.multi_bulk_length = 0;
    this.multi_bulk_replies = null;
    this.multi_bulk_pos = 0;

    if (this.multi_bulk_nested_length === this.multi_bulk_nested_pos) {
      // for nested multi bulk replies.
      if (this.deep_nesting) {
        this.multi_array[this.multi_array_pos] = this.multi_bulk_nested_replies;
        this.multi_array_pos += 1;

        if (this.multi_array_pos != this.multi_array_length) {
          this.multi_bulk_nested_length = this.multi_array_length;
          this.multi_bulk_nested_pos = this.multi_array_pos;
          return;
        }
        this.multi_bulk_nested_replies = this.multi_array;
        this.deep_nesting = false;
      }
      this.emit("reply", this.multi_bulk_nested_replies);
      this.multi_bulk_nested_length = 0;
      this.multi_bulk_nested_pos = 0;
      this.multi_bulk_nested_replies = null;
    }
  } else {
    this.emit("reply", this.multi_bulk_replies);
    this.multi_bulk_length = 0;
    this.multi_bulk_replies = null;
    this.multi_bulk_pos = 0;
  }
=======
    this._offset = 0;
};

ReplyParser.prototype.parseHeader = function () {
    var end   = this._packetEndOffset(),
        value = small_toString(this._buffer, this._offset, end - 1);

    this._offset = end + 1;

    return value;
};

ReplyParser.prototype._packetEndOffset = function () {
    var offset = this._offset;

    while (this._buffer[offset] !== 0x0d && this._buffer[offset + 1] !== 0x0a) {
        offset++;

        if (offset >= this._buffer.length) {
            throw new Error("didn't see LF after NL reading multi bulk count (" + offset + " => " + this._buffer.length + ", " + this._offset + ")");
        }
    }

    offset++;
    return offset;
>>>>>>> origin/master
};

ReplyParser.prototype._bytesRemaining = function () {
    return (this._buffer.length - this._offset) < 0 ? 0 : (this._buffer.length - this._offset);
};

ReplyParser.prototype.parser_error = function (message) {
    this.emit("error", message);
};

ReplyParser.prototype.send_error = function (reply) {
    this.emit("reply error", reply);
};

ReplyParser.prototype.send_reply = function (reply) {
    this.emit("reply", reply);
};
