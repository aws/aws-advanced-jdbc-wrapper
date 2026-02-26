package com.example.encryptspring;

import org.springframework.web.bind.annotation.*;
import java.util.List;

@RestController
@RequestMapping("/users")
public class UserController {
    private final UserRepository repository;
    
    public UserController(UserRepository repository) {
        this.repository = repository;
    }
    
    @GetMapping
    public List<User> getAll() {
        return repository.findAll();
    }
    
    @PostMapping
    public User create(@RequestBody User user) {
        return repository.save(user);
    }
    
    @PutMapping("/{id}")
    public User update(@PathVariable Long id, @RequestBody User user) {
        user.setId(id);
        return repository.save(user);
    }
    
    @DeleteMapping("/{id}")
    public void delete(@PathVariable Long id) {
        repository.deleteById(id);
    }
}
