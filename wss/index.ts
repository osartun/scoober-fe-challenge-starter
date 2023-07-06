import express from "express";
import * as http from "http";
import SocketIO, { Socket } from "socket.io";

import APIService from "./api.service";

const port = 8082;
const server = http.createServer(express);
const io = new SocketIO.Server(server);
const apiService = new APIService();

enum GameState {
  WAIT = "wait",
  PLAY = "play",
}

io.on("connection", (socket: Socket) => {
  console.log('Socket has connected with ID:', socket.id);

  socket.on("login", ({ username }) => {
    console.log(`Login with ${username}`);
    apiService
      .createUser(socket.id, username)
      .then(() => {
        socket.emit("message", {
          user: username,
          message: `Welcome ${username}`,
          socketId: socket.id,
        });
      })
      .catch((err) => {
        socket.emit("error", { ...err });
        console.log(err);
      });
  });

  /* Join to the room */
  socket.on("joinRoom", ({ username, room, roomType }) => {
    apiService
      .assignRoom(room, socket.id, roomType)
      .then(() => {
        socket.emit("message", {
          user: username,
          message: `welcome to room ${room}`,
          room: room,
        });
        if (roomType !== "cpu") {
          socket.broadcast.to(room).emit("message", {
            user: username,
            message: `has joined ${room}`,
            room: room,
          });
        }

        /* Check the room with how many socket is connected */
        const maxRoomSize = roomType === "cpu" ? 1 : 2;
        const emitOnReady = () => {
          if (
            io.of("/").adapter.rooms.has(room) &&
            io.of("/").adapter.rooms.get(room)?.size === maxRoomSize
          ) {
            io.to(room).emit("onReady", { state: true });
          } else {
            io.to(room).emit("onReady", { state: false });
          }
        }
        const promise = socket.join(room);
        if (promise) {
          promise.then(emitOnReady);
        } else {
          emitOnReady();
        }
      })
      .catch((err) => {
        socket.emit("error", { ...err });
        console.log(err);
      });
  });

  /* Start the game and send the first random number with turn control */
  socket.on("letsPlay", () => {
    apiService
      .getUserDetail(socket.id)
      .then((result) => {
        io.to(result?.data.room).emit("randomNumber", {
          number: `${apiService.createRandomNumber(1999, 9999)}`,
          isFirst: true,
        });

        const usersInRoom = io.of("/").adapter.rooms.get(result?.data.room);
        socket.to(result?.data.room).emit("activateYourTurn", {
          user: usersInRoom?.values[0] ?? null,
          state: GameState.WAIT,
        });
        socket.emit("activateYourTurn", {
          user: socket.id,
          state: GameState.PLAY,
        });
      })
      .catch((err) => {
        socket.emit("error", { ...err });
        console.log(err);
      });
  });

  /* Send Calculated number back with Divisible control */
  socket.on("sendNumber", (payload) => {
    apiService.getUserDetail(socket.id).then((result) => {
      const number = Number(payload.number);
      const selectedNumber = Number(payload.selectedNumber);
      const numbers = [selectedNumber, number];
      const sumValues = (num: number[]) => {
        return num.reduce((a: number, b: number) => {
          return a + b;
        });
      };
      let cpuPlayerTimeout: NodeJS.Timeout | undefined = undefined;

      const calculationResult = (number: number[], numberB: number): number => {
        const res = sumValues(number);
        if (res % 3 == 0) {
          return res / 3;
        } else {
          return numberB;
        }
      };

      const lastResult = calculationResult(numbers, number);

      // When the second oponnent is a CPU
      if (result?.data?.roomType === "cpu") {
        // After clients selection it will wait 2 seconds for the CPU selection

        cpuPlayerTimeout = setTimeout(() => {
          const setOfRandomNumbers = [1, 0, -1];
          const randomCPU =
            setOfRandomNumbers[
              Math.floor(Math.random() * setOfRandomNumbers.length)
            ];
          const combinedNumbers = [randomCPU, lastResult];
          const CPUResult = calculationResult(combinedNumbers, lastResult);
          io.to(result?.data.room).emit("randomNumber", {
            number: calculationResult(combinedNumbers, lastResult),
            isFirst: false,
            user: "CPU",
            selectedNumber: randomCPU,
            isCorrectResult: CPUResult == lastResult ? false : true,
          });

          io.to(result?.data.room).emit("activateYourTurn", {
            user: socket.id,
            state: GameState.PLAY,
          });

          if (calculationResult(combinedNumbers, lastResult) === 1) {
            io.to(result?.data.room).emit("gameOver", {
              user: "CPU",
              isOver: true,
            });
          }
        }, 2000);
      }
      io.to(result?.data.room).emit("randomNumber", {
        number: calculationResult(numbers, number),
        isFirst: false,
        user: result?.data.name,
        selectedNumber: selectedNumber,
        isCorrectResult:
          calculationResult(numbers, number) == number ? false : true,
      });

      socket.emit("activateYourTurn", {
        user: socket.id,
        state: GameState.WAIT,
      });

      if (result?.data.roomType === "human") {
        const usersInRoom = io.of("/").adapter.rooms.get(result?.data.room);
        const opposingUser = usersInRoom ? Array.from(usersInRoom).find(userSocketId => userSocketId !== socket.id) : '';

        socket.to(result?.data.room).emit("activateYourTurn", {
          user: opposingUser,
          state: GameState.PLAY,
        });
      }

      /* if 1 is reached then emit the GameOver Listener */
      if (Math.abs(calculationResult(numbers, number)) == 1) {
        io.to(result?.data.room).emit("gameOver", {
          user: result?.data.name,
          isOver: true,
        });
        if (cpuPlayerTimeout) clearTimeout(cpuPlayerTimeout);
      }
    });
  });

  /* Clear all data and states when the user leave the room */
  socket.on("leaveRoom", () => {
    apiService.getUserDetail(socket.id).then((result) => {
      io.to(result?.data.room).emit("onReady", { state: false });
      apiService.removeUserFromRoom(socket.id).then(() => {
        socket.leave(result?.data.room);
      });
    });
  });

  /* OnDisconnet clear all login and room data from the connected socket */
  socket.on("disconnect", () => {
    console.log('Socket disconnects. ID: ', socket.id);
    apiService.getUserDetail(socket.id).then((result) => {
      socket.broadcast.to(result?.data.room).emit("onReady", { state: false });
      apiService.removeUserFromRoom(socket.id).then(() => {
        socket.leave(result?.data.room);
      });
    });

    // Clear selected user from FakeDb and broadcast the event to the subscribers
    apiService.clearUser(socket.id).then(() => {
      socket.broadcast.emit("listTrigger", `${true}`);
    });
  });
});

server.listen(port, () => {
  console.log(
    `Socket Connection Established on ${process.env.HOST_LOCAL} in port ${process.env.SOCKET_PORT}`
  );
});
