#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov 19 13:30:08 2023

@author: nicemicro

Acknowledgement:
Credit to Lime Parallelogram for the pyIR library, that was used as reference.
https://github.com/Lime-Parallelogram/pyIR
"""

from xml.etree import ElementTree as et
from threading import Thread
import time
from queue import Queue
from typing import Callable, Optional
from datetime import datetime
try:
    import RPi.GPIO as GPIO
except ImportError:
    GPIO = None


class IrReceiver(Thread):
    def __init__(self, sensor_pin: int, commands: Queue, ir_pulses: Queue):
        Thread.__init__(self)
        self.sensor_pin: int = sensor_pin
        self.commands: Queue = commands
        self.ir_pulses: Queue = ir_pulses

    def run(self):
        exitFlag: bool = False
        ir_raw_input: list[tuple[int, float]] = []
        inputvalue: bool = True
        countdown: int = 0
        while not exitFlag:
            inputvalue = GPIO.input(self.sensor_pin)
            countdown = 1000
            while countdown > 0 and inputvalue:
                time.sleep(0.0001)
                countdown -= 1
                inputvalue = GPIO.input(self.sensor_pin)
            if not inputvalue:
                ir_raw_input = self.get_raw()
                self.ir_pulses.put(ir_raw_input)
                #print(ir_raw_input)
            if self.commands.empty():
                continue
            funct = self.commands.get()
            if funct == "quit":
                exitFlag = True
        print("IR sensor watcher quits")

    def get_raw(self) -> list[tuple[int, float]]:
        high_count: int = 0 # Number of consecutive measurements of high state
        ir_raw_input: list[tuple[int, float]] = [] # list of pulses and timings
        prev_val: int = False # The previous pin state

        curr_val: int = GPIO.input(self.sensor_pin) # Current pin state

        start: datetime = datetime.now() # Sets start time
        now: datetime

        while True:
            if curr_val != prev_val:
                now = datetime.now()
                pulse_length = (now - start).microseconds
                start = now # Reseting timer
                ir_raw_input.append((prev_val, pulse_length))
            # Interrupts code if an extended high period is detected
            # A repeat code might be sent after the break, but we just
            # going to handle that separately.
            if curr_val:
                high_count += 1
                if high_count >= 1000:
                    high_count = 0
                    if (datetime.now() - start).microseconds > 10000:
                        break
            # Reads values again
            prev_val = curr_val
            curr_val = GPIO.input(self.sensor_pin)
        return ir_raw_input


class IrReceiverHandler():
    def __init__(self, controller, pin_num: int = 11) -> None:
        self.controller = None
        if GPIO is None:
            return
        tree = et.parse("remote.xml")
        root = tree.getroot()
        assert root.tag == "MusicAutoplay_Data", "The XML file does not match."
        if "version" in root.attrib:
            print("Loading file created with version ", root.attrib["version"])
        else:
            print("File version unknown.")
        self.buttons: dict[int, dict[str, str]] = {}
        protocol: str = ""
        button_code: int
        button_cmd: Optional[str]
        for record in root:
            if record.tag == "Protocol":
                assert isinstance(record.text, str)
                protocol = record.text
                continue
            if record.tag == "Button":
                button_text = record.attrib["text"]
                for button_data in record:
                    if button_data.tag == "code":
                        assert isinstance(button_data.text, str)
                        button_code = int(button_data.text)
                        continue
                    if button_data.tag == "command":
                        button_cmd = button_data.text
                if not isinstance(button_cmd, str):
                    continue
                self.buttons[button_code] = {
                    "text": button_text,
                    "cmd": button_cmd
                }
        self.controller = controller

        decoders: dict[str, Callable] = {
            "NEC": NEC_decode
        }
        self.decoder = decoders[protocol]
        GPIO.setmode(GPIO.BOARD)
        self.sensor_pin = pin_num
        GPIO.setup(self.sensor_pin, GPIO.IN)

        self.commands: Queue = Queue()
        self.ir_pulses: Queue = Queue()
        self.ir_receiver = IrReceiver(self.sensor_pin, self.commands, self.ir_pulses)
        self.ir_receiver.start()
        self.last_command: int = -1
        self.last_repeated: bool = False

    def check_queue(self) -> None:
        if self.controller is None:
            return
        while not self.ir_pulses.empty():
            ir_pulse: int = self.ir_pulses.get()
            repeater: bool = False
            try:
                ir_code = self.decoder(ir_pulse)
            except ValueError:
                continue
            if ir_code == -1:
                if not self.last_repeated:
                    self.last_repeated = True
                    # First repeat code is ignored
                    continue
                ir_code = self.last_command
                repeater = True
            else:
                self.last_repeated = False
                self.last_command = ir_code
            if ir_code not in self.buttons:
                continue
            command: str = self.buttons[ir_code]["cmd"]
            if command == "vol+":
                self.controller.volume(5)
                continue
            if command == "vol-":
                self.controller.volume(-5)
                continue
            if repeater:
                continue
            if command == "shutdown":
                self.controller.destroy()
                continue
            if command == "playpause":
                self.controller.play_pause()
                continue
            if command == "next":
                self.controller.play_next(-1)

    def destroy(self) -> None:
        if self.controller is None:
            return
        self.commands.put("quit")
        self.ir_receiver.join()

def NEC_decode(data: list[tuple[int, float]]) -> int:
    message: int = 0
    #messagestr: str = ""
    # NEC ref: https://techdocs.altium.com/display/FPGA/NEC%2bInfrared%2bTransmission%2bProtocol
    # (high and low are reversed on the RPi compared to the reference)
    # NEC code starts with 9 ms low pulse (but it might have been realized
    # too late, so shorter time is acceptable)
    lowhigh, timegap = data.pop(0)
    if lowhigh or timegap < 5000 or timegap > 11000:
        raise ValueError("Data is not formatted in accordance with NEC")
    lowhigh, timegap = data.pop(0)
    # A repeat code (button is kept pressed down) gets a 2.25 ms high pulse
    if lowhigh and 1500 < timegap and timegap < 3000:
        lowhigh, timegap = data.pop(0)
        # Repeat code ends with 0.5625 ms low pulse
        if lowhigh or timegap > 1500:
            raise ValueError("Data is not formatted in accordance with NEC")
        else:
            #print("Repeat code received")
            return -1
    # Message is prefixed by 4.5 ms high pulse
    if not lowhigh or timegap < 3500 or timegap > 5500:
        raise ValueError("Data is not formatted in accordance with NEC")
    for (lowhigh, timegap) in data:
        if lowhigh: # NEC has constant low periods, data is in high periods
            if timegap > 1000: # 562.5 us is zero, 1.25 ms is a one
                #messagestr += "1"
                message = message * 2 + 1
            else:
                #messagestr += "0"
                message *= 2
    #print(messagestr)
    return message
